extern crate memmap;
extern crate parking_lot;

mod volatile;

use std::{io, mem, ptr};
use std::marker::PhantomData;
use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::File;

use memmap::{Mmap, Protection};
use parking_lot::Mutex;

use volatile::Volatile;

const RANKS: usize = 128;

pub struct DiskArray<T: Volatile> {
    ranks: [UnsafeCell<Option<Mmap>>; RANKS],
    initialized: AtomicUsize,
    rank_writelock: Mutex<()>,
    path: PathBuf,
    len: AtomicUsize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Volatile> Sync for DiskArray<T> {}
unsafe impl<T: Volatile> Send for DiskArray<T> {}

fn min_max(rank: usize) -> (usize, usize) {
    if rank == 0 {
        (0, 0)
    } else {
        (2usize.pow(rank as u32) - 1, 2usize.pow(rank as u32 + 1) - 2)
    }
}

fn rank_ofs(index: usize) -> (usize, usize) {
    let index = index + 1;
    let rank = mem::size_of::<usize>() * 8 - index.leading_zeros() as usize - 1;
    (rank, index - 2usize.pow(rank as u32))
}

impl<T: Volatile> DiskArray<T> {
    pub fn new<P: Into<PathBuf> + Clone>(path: P) -> io::Result<Self> {
        unsafe {
            #[cfg(not(release))]
            {
                let z: T = mem::zeroed();
                assert!(z == T::ZEROED, "Invalid Volatile implementation");
            }

            let mut ranks: [UnsafeCell<Option<Mmap>>; RANKS] =
                mem::uninitialized();

            for i in 0..RANKS {
                ptr::write(&mut ranks[i], UnsafeCell::new(None))
            }

            let mut n_ranks = 0;
            for rank in 0..RANKS {
                let mut rank_path = path.clone().into();
                rank_path.push(format!("{}", rank));
                if rank_path.exists() {
                    n_ranks += 1;
                    let mmap =
                        Mmap::open_path(&rank_path, Protection::ReadWrite)?;
                    *ranks[rank].get() = Some(mmap);
                } else {
                    break;
                }
            }

            let mut len = 0;
            if n_ranks > 0 {
                let (mut min, mut max) = min_max(n_ranks - 1);
                loop {
                    let probe = min + (max - min) / 2;
                    let (rank, ofs) = rank_ofs(probe);

                    let ptr: *const T = mem::transmute(
                        (*ranks[rank].get())
                            .as_ref()
                            .expect("accessing uninitialized rank")
                            .ptr(),
                    );
                    let ptr = ptr.offset(ofs as isize);
                    if *ptr != T::ZEROED {
                        // found something
                        if min == max {
                            len = min + 1;
                            break;
                        }
                        min = probe + 1;
                    } else {
                        if min == max {
                            len = min;
                            break;
                        }
                        max = probe;
                    }
                }
            }

            Ok(DiskArray {
                ranks,
                len: AtomicUsize::new(len),
                initialized: AtomicUsize::new(n_ranks),
                rank_writelock: Mutex::new(()),
                path: path.into(),
                _marker: PhantomData,
            })
        }
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        let (rank, ofs) = rank_ofs(idx);
        if rank < self.initialized.load(Ordering::Relaxed) {
            unsafe {
                let ptr: *const T = mem::transmute(
                    (*self.ranks[rank].get())
                        .as_ref()
                        .expect("accessing uninitialized rank")
                        .ptr(),
                );
                let ptr = ptr.offset(ofs as isize);
                if *ptr == T::ZEROED {
                    None
                } else {
                    Some(mem::transmute(ptr))
                }
            }
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn push(&self, t: T) -> io::Result<usize> {
        #[cfg(not(release))]
        assert!(t != T::ZEROED, "Cannot insert zeroes!");

        let idx = self.len.fetch_add(1, Ordering::Relaxed);
        let (rank, ofs) = rank_ofs(idx);

        if rank >= self.initialized.load(Ordering::Relaxed) {
            let _rank_writelock = self.rank_writelock.lock();
            // is the rank still too small after aquiring the lock?
            if rank >= self.initialized.load(Ordering::Relaxed) {
                let mut path = self.path.clone();
                path.push(format!("{:?}", rank));
                let file = File::create(&path)?;
                let n_elements = 2usize.pow(rank as u32);
                let size = mem::size_of::<T>() * n_elements;
                file.set_len(size as u64)?;
                let mmap = Mmap::open_path(&path, Protection::ReadWrite)?;
                unsafe { *self.ranks[rank].get() = Some(mmap) }
                self.initialized.fetch_add(1, Ordering::Relaxed);
            }
        }

        unsafe {
            let ptr: *const T = mem::transmute(
                (*self.ranks[rank].get())
                    .as_ref()
                    .expect("accessing uninitialized rank")
                    .ptr(),
            );
            let ptr: *const T = ptr.offset(ofs as isize);
            let ptr: &mut T = mem::transmute(ptr);
            ptr::write(ptr, t);
            Ok(idx)
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    use super::*;
    use self::tempdir::TempDir;
    use self::std::sync::Arc;
    use self::std::thread;
    const N: usize = 100_000;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct CheckSummedUsize {
        val: usize,
        checksum: usize,
    }

    impl CheckSummedUsize {
        fn new(val: usize) -> Self {
            CheckSummedUsize {
                val,
                checksum: val + 1,
            }
        }
    }

    impl Volatile for CheckSummedUsize {
        const ZEROED: Self = CheckSummedUsize {
            val: 0,
            checksum: 0,
        };
    }

    #[test]
    fn simple_diskarray() {
        let tempdir = TempDir::new("diskarray").unwrap();
        let array = DiskArray::new(tempdir.path()).unwrap();

        for i in 0..N {
            assert_eq!(array.push(CheckSummedUsize::new(i)).unwrap(), i);
        }

        for i in 0..N {
            assert_eq!(array.get(i).unwrap(), &CheckSummedUsize::new(i))
        }

        assert_eq!(array.get(N), None)
    }

    #[test]
    fn diskarray_restore() {
        let tempdir = TempDir::new("diskarray").unwrap();

        {
            let array = DiskArray::new(tempdir.path()).unwrap();

            for i in 0..N {
                assert_eq!(array.push(CheckSummedUsize::new(i)).unwrap(), i);
            }
        }

        {
            let array =
                DiskArray::<CheckSummedUsize>::new(tempdir.path()).unwrap();

            for i in 0..N {
                assert_eq!(array.get(i).unwrap(), &CheckSummedUsize::new(i))
            }
        }
    }

    #[test]
    fn diskarray_len() {
        for little_n in 0..100 {
            let tempdir = TempDir::new("diskarray").unwrap();
            {
                let array = DiskArray::new(tempdir.path()).unwrap();

                for i in 0..little_n {
                    assert_eq!(
                        array.push(CheckSummedUsize::new(i)).unwrap(),
                        i
                    );
                }
            }

            {
                let array =
                    DiskArray::<CheckSummedUsize>::new(tempdir.path()).unwrap();

                assert_eq!(array.len(), little_n);
            }
        }
    }

    #[test]
    fn stress() {
        let tempdir = TempDir::new("diskarray").unwrap();

        let array = Arc::new(DiskArray::new(tempdir.path()).unwrap());

        let n_threads = 16;
        let mut handles = vec![];

        for thread in 0..n_threads {
            let array = array.clone();
            handles.push(thread::spawn(move || for i in 0..N {
                if i % n_threads == thread {
                    array.push(CheckSummedUsize::new(i)).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(array.len(), N);
    }

    #[test]
    fn mapping() {
        let mappings: Vec<_> = (0..7).map(rank_ofs).collect();
        assert_eq!(
            mappings,
            vec![(0, 0), (1, 0), (1, 1), (2, 0), (2, 1), (2, 2), (2, 3)]
        );
    }

    #[test]
    fn min_max_index() {
        let mappings: Vec<_> = (0..5).map(min_max).collect();
        assert_eq!(mappings, vec![(0, 0), (1, 2), (3, 6), (7, 14), (15, 30)]);
    }
}
