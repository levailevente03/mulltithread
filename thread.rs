use std::array;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::marker::Tuple;
use std::num::NonZeroU64;
use std::sync::atomic::AtomicPtr;
use std::sync::{Arc, Mutex, Weak, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use rand::rng;
use rand::seq::IteratorRandom;

const TICKER_SIZE: usize = 32;
const CHECKER_ERROR_SIZE: u32 = 100;
const REPEATER_JUNK: u32 = 100_000;



#[derive(Debug)]
pub struct TickerController{
    threads: [HashMap<NonZeroU64, TickerInsider>; TICKER_SIZE],
    timer: Box<dyn TickerTimer>,
    speed: u128,
    tick: u32,
    cycle: u64,
    access: AtomicPtr<TickerControllerAccess>
}

impl TickerController{
    pub fn new(run_speed: u128) -> Self{
        Self{
            threads: array::from_fn(|_| HashMap::new()),
            timer: Box::new(ZeroSleepTimer::new(run_speed)),
            speed: run_speed,
            tick: 0,
            cycle: 0,
            access: unsafe {std::mem::zeroed()}
        }
    }

    pub fn give_access(&mut self, mut access: Arc<TickerControllerAccess>){
        unsafe{self.access = AtomicPtr::new(Arc::get_mut_unchecked(&mut access));}
    }

    pub fn set_tick(&mut self, tick: u32, cycle: u64){
        self.tick = tick;
        self.cycle = cycle;
    }

    pub fn get_tick(&self) -> u32{
        self.tick
    }

    pub fn get_cycle(&self) -> u64{
        self.cycle
    }

    pub fn get_full_tick(&self) -> (u64, u32){
        (self.cycle, self.tick)
    }

    pub fn tick_one(&self){
        for i in 0..TICKER_SIZE{
            for insider in self.threads[i].values(){
            insider.ticker.lock().unwrap().tick();
        }
        }
    }

    pub fn run(&mut self, exit: Arc<bool>, stop: Arc<bool>){
        self.timer.now();
        loop{
            if *exit{
                for i in 0..TICKER_SIZE{
                    for insider in self.threads[i].values(){
                        unsafe {
                            let mut a = insider.stop.clone();
                            *Arc::get_mut_unchecked(&mut a) = true;
                            insider.thread.thread().unpark();
                        }
                    }
                }
                break;
            }
            if *stop{
                break;
            }
            let mut exited = Vec::new();
            for i in 0..TICKER_SIZE{
                for (key, insider) in self.threads[i].iter_mut(){
                    insider.inner_counter += insider.timer;
                    insider.checker_counter += 1;
                    /*match insider.checker.try_lock(){
                        Ok(mut guard) => {
                            *guard += insider.checker_counter;
                            if *guard >= insider.checker_error_size{
                                //println!("{} reached error check", key);
                                let mut a = insider.ticker.clone();
                                unsafe{
                                    let ticker = Arc::get_mut_unchecked(&mut a).get_mut().unwrap();
                                    let index = ticker.get_savestate_index();
                                    if ticker.get_savestate(){
                                        let vec = ticker.get_savestate_items();
                                        if vec.len() > 0{
                                            match vec[index].clone().upgrade(){
                                                Some(_item) => {
                                                    
                                                }
                                                None => {
                                                    //println!("Object is None");
                                                    ticker.clear_poison();
                                                    
                                                }
                                            }
                                        }
                                        //println!("Category {}", i);
                                    } else {
                                        //println!("The ticker is not running");
                                    }
                                }
                            }
                            insider.checker_counter = 0;
                        }
                        Err(_) => ()
                    }*/
                    
                    match insider.counter.try_lock(){
                        Ok(mut guard) => {
                            *guard += insider.inner_counter;
                            insider.inner_counter = 0.;
                        }
                        Err(_) => ()
                    }
                    if insider.thread.is_finished(){
                        exited.push((i, *key));
                    }
                    insider.thread.thread().unpark();
                }
            }
            for (i, t) in exited{
                println!("Thread finished in {} category. Process ID: {}", i, t);
                let exited = self.threads[i].remove(&t).unwrap();
                exited.thread.join().expect("Thread joining failed")
            }
            self.tick += 1;
            if self.tick == 0{
                self.cycle += 1;
            }
            let ptr = self.access.get_mut();
            if !ptr.is_null(){
                unsafe {
                    let access = &mut **ptr;
                    access.tick = self.tick;
                    access.cycle = self.cycle;
                }
            }
            self.timer.wait();
        }
    }

    pub fn add_task(&mut self, task: Arc<dyn TickerPackage>){
        let key = self.threads[task.category()].keys().choose(&mut rng());
        if key.is_some(){
            let key = key.copied().unwrap();
            self.threads[task.category()].get_mut(&key).unwrap().buffer.lock().unwrap().push(task);
        }
    }

    pub fn create_repeater(&mut self, name: String, run_speed: u128, category: usize) -> Result<(), std::io::Error>{
        let ticker = Arc::new(Mutex::new(TickerRepeater::new(run_speed, true)));
        let runs = Arc::new(Mutex::new(0.0));
        let t = TickerThread::new(ticker.clone(), runs.clone());
        let stop = Arc::new(false);
        let s = stop.clone();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let b = buffer.clone();
        let checker = Arc::new(Mutex::new(0));
        let c= checker.clone();
        match std::thread::Builder::new()
        .name(name)
        .spawn(move || ticker_thread_fn(t, s, b, c)){
            Ok(handle)  => {
                let id = handle.thread().id().as_u64();
                self.threads[category].insert(id, 
                    TickerInsider { thread: handle, stop, ticker, timer: (run_speed as f64 / self.speed as f64), counter: runs, inner_counter: 0., buffer, checker, checker_counter: 0, checker_error_size: ((self.speed as f64 / run_speed as f64) * CHECKER_ERROR_SIZE as f64) as u32 }
                );
                Ok(())
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn create_repeateralt(&mut self, name: String, run_speed: u128, category: usize) -> Result<(), std::io::Error>{
        let ticker = Arc::new(Mutex::new(TickerRepeaterAlt::new(run_speed, true)));
        let runs = Arc::new(Mutex::new(0.0));
        let t = TickerThread::new(ticker.clone(), runs.clone());
        let stop = Arc::new(false);
        let s = stop.clone();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let b = buffer.clone();
        let checker = Arc::new(Mutex::new(0));
        let c= checker.clone();
        match std::thread::Builder::new()
        .name(name)
        .spawn(move || ticker_thread_fn(t, s, b, c)){
            Ok(handle)  => {
                let id = handle.thread().id().as_u64();
                self.threads[category].insert(id, 
                    TickerInsider { thread: handle, stop, ticker, timer: (run_speed as f64 / self.speed as f64), counter: runs, inner_counter: 0., buffer, checker, checker_counter: 0, checker_error_size: ((self.speed as f64 / run_speed as f64) * CHECKER_ERROR_SIZE as f64) as u32 }
                );
                Ok(())
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn create_default(&mut self, name: String, run_speed: u128, category: usize) -> Result<(), std::io::Error>{
        let ticker = Arc::new(Mutex::new(TickerBasic::new(run_speed, true)));
        let runs = Arc::new(Mutex::new(0.0));
        let t = TickerThread::new(ticker.clone(), runs.clone());
        let stop = Arc::new(false);
        let s = stop.clone();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let b = buffer.clone();
        let checker = Arc::new(Mutex::new(0));
        let c= checker.clone();
        ticker.clone().lock().unwrap().cycle_adder(ticker.clone());
        match std::thread::Builder::new()
        .name(name)
        .spawn(move || ticker_thread_fn(t, s, b, c)){
            Ok(handle)  => {
                let id = handle.thread().id().as_u64();
                self.threads[category].insert(id, 
                    TickerInsider { thread: handle, stop, ticker, timer: (run_speed as f64 / self.speed as f64), counter: runs, inner_counter: 0., buffer, checker, checker_counter: 0, checker_error_size: ((self.speed as f64 / run_speed as f64) * CHECKER_ERROR_SIZE as f64) as u32 }
                );
                Ok(())
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn create_new<Args: Tuple + Send + 'static, F: Fn<Args> + Send + 'static>(
        &mut self, 
        f: Box<F>, 
        args: Args, 
        name: String, 
        run_speed: u128,
        run_time_counter: Arc<Mutex<f64>>,
        stop: Arc<bool>,
        ticker: Arc<Mutex<TickerBasic>>,
        buffer: Arc<Mutex<Vec<Arc<dyn TickerPackage>>>>,
        category: usize,
        checker: Arc<Mutex<u32>>
    ) -> Result<(), std::io::Error> where <F as FnOnce<Args>>::Output: Send
    {
        ticker.clone().lock().unwrap().cycle_adder(ticker.clone());
        match std::thread::Builder::new()
        .name(name)
        .spawn(move || {Fn::call(&f, args);}){
            Ok(handle)  => {
                let id = handle.thread().id().as_u64();
                self.threads[category].insert(id, 
                    TickerInsider { thread: handle, stop, ticker, timer: (run_speed as f64 / self.speed as f64), counter: run_time_counter, inner_counter: 0., buffer, checker, checker_counter: 0, checker_error_size: ((self.speed as f64 / run_speed as f64) * CHECKER_ERROR_SIZE as f64) as u32 }
                );
                Ok(())
            }
            Err(error) => {
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TickerThread{
    runtimes: Arc<Mutex<f64>>,
    ticker: Arc<Mutex<dyn Ticker>>
}

impl TickerThread{
    pub fn new(ticker: Arc<Mutex<dyn Ticker>>, runtimes: Arc<Mutex<f64>>) -> Self{
        Self{
            runtimes,
            ticker
        }
    }

    pub fn get_target(&self) -> u128{
        self.ticker.lock().unwrap().get_speed()
    }

    pub fn ticker(&self) -> &Arc<Mutex<dyn Ticker>>{
        &self.ticker
    }

    pub fn get_name(&self) -> String{
        ::std::thread::current().name().unwrap_or("UNNAMED").to_string()
    }

    pub fn get_id(&self) -> u32{
        ::std::process::id()
    }

    pub fn tick(&mut self){
        self.ticker.lock().unwrap().tick();
    }

    pub fn cycle(&mut self){
        loop {
            if {
                let mut num = self.runtimes.lock().unwrap();
                if *num >= 1.0{
                    self.ticker.lock().unwrap().tick();
                    *num = *num - 1.0;
                    false
                } else {true}
            }{break;}
        }
    }
}

pub fn ticker_thread_fn(mut ticker_thread: TickerThread, stop: Arc<bool>, buffer: Arc<Mutex<Vec<Arc<dyn TickerPackage>>>>, checker: Arc<Mutex<u32>>){
    loop {
        if *stop {break;}
        ticker_thread.cycle();
        let a = ticker_thread.ticker();
        let mut t = a.lock().unwrap();
        let mut v = buffer.lock().unwrap();
        for i in v.drain(..){
            t.add_event(i);
        }
        drop(v);
        drop(t);
        drop(a);
        //*checker.lock().unwrap() = 0;
        ::std::thread::park();
    }
}

pub trait TickerTimer: Send + Sync{
    fn now(&mut self);
    fn get_target(&self) -> u128;
    fn set_target(&mut self, target: u128);
    fn get_counter(&self) -> u128;
    fn sleep(&self);
    fn wait(&mut self);
}

impl Debug for dyn TickerTimer{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TickerTimer {{ target: {} }}", self.get_target())
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct ZeroSleepTimer{
    timer: Instant,
    counter: u128,
    target: u128,
}

impl ZeroSleepTimer{
    pub fn new(target: u128) -> Self{
        Self{
            timer: Instant::now(),
            counter: 0,
            target: 1_000_000_000 / target
        }
    }
}

impl TickerTimer for ZeroSleepTimer{
    fn now(&mut self){
        self.timer = Instant::now();
    }

    fn get_counter(&self) -> u128{
        self.counter
    }

    fn get_target(&self) -> u128{
        self.target
    }

    fn set_target(&mut self, target: u128){
        self.target = target;
    }

    fn sleep(&self){
        ::std::thread::sleep(const {Duration::from_nanos(0)});
    }

    fn wait(&mut self){
        loop{
            let c = self.timer.elapsed().as_nanos();
            if c >= 1000 {
                self.counter += c;
                let call = if self.counter <= self.target {true} else {false};
                self.timer = Instant::now();
                if call {
                    self.sleep();
                } else {
                    self.counter -= self.target;
                    break;
                }
            }
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SleepTimer{
    timer: Instant,
    target: Duration,
}

impl SleepTimer{
    pub fn new(target: Duration) -> Self{
        Self { 
            timer: Instant::now(), 
            target 
        }
    }
}

impl TickerTimer for SleepTimer{
    fn get_counter(&self) -> u128 {0}

    fn get_target(&self) -> u128 {
        self.target.as_nanos()
    }

    fn set_target(&mut self, target: u128) {
        self.target = Duration::from_nanos(target as u64);
    }

    fn now(&mut self) {
        self.timer = Instant::now();
    }

    fn sleep(&self) {
        ::std::thread::sleep(self.target);
    }

    fn wait(&mut self) {
        if self.timer.elapsed() < self.target{
            let sleep = self.target.as_micros() - self.timer.elapsed().as_micros();
            ::std::thread::sleep(Duration::from_nanos(sleep as u64));
        }
    }
}

pub trait TickerObject: Send + Sync{
    fn call(self: Box<Self>);
    fn repeat(&self) {}
    fn junk(&self) -> bool {true}
}

impl Debug for dyn TickerObject{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TickerObject")       
    }
}

pub trait TickerPackage: Send + Sync{
    fn delay(&self) -> u32;
    fn request(&self) -> u32;
    fn set_request(&mut self, request: u32);
    fn persistent(&self) -> bool;
    fn is_failure(&self) -> bool;
    fn category(&self) -> usize;
    fn call(self: Arc<Self>) -> bool;
    fn junk(self: Arc<Self>) -> bool {Arc::weak_count(&self) == 0 && !self.persistent()}
    fn to_weak(self: &Arc<Self>) -> Weak<Self> where Self: Sized {Arc::downgrade(self)}
}

impl Debug for dyn TickerPackage{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TickerPackage {{ delay: {}, request: {}, persistent: {}, failure: {}, category: {} }}", self.delay(), self.request(), self.persistent(), self.is_failure(), self.category())
    }
}

#[derive(Debug)]
pub struct TickerBox{
    object: Box<dyn TickerObject>,
    delay: u32,
    request: u32,
    persistent: bool,
    failure: bool,
    category: usize
}

impl TickerBox{
    pub fn new(object: Box<dyn TickerObject>, delay: u32, persistent: bool, category: usize) -> Self{
        Self{
            object,
            delay, 
            request: 0,
            persistent,
            failure: false,
            category
        }
    }
}

impl TickerPackage for TickerBox{
    fn call(self: Arc<Self>) -> bool{
        if self.persistent || Arc::weak_count(&self) > 1{
            match Arc::try_unwrap(self){
                Ok(b) => {b.object.call(); true}
                Err(failed) => {
                    unsafe {
                        let mut a = failed.clone();
                        Arc::get_mut_unchecked(&mut a).failure = true;
                    }
                    false
                }
            }
        } else {true}
    }

    fn delay(&self) -> u32{
        self.delay
    }

    fn request(&self) -> u32{
        self.request
    }

    fn set_request(&mut self, request: u32) {
        self.request = request;
    }

    fn persistent(&self) -> bool{
        self.persistent
    }

    fn is_failure(&self) -> bool{
        self.failure
    }

    fn category(&self) -> usize{
        self.category
    }
}

#[derive(Debug)]
pub struct TickerEternal{
    object: Box<dyn TickerObject>,
    persistent: bool,
    failure: bool,
    category: usize
}

impl TickerEternal{
    pub fn new(object: Box<dyn TickerObject>, persistent: bool, category: usize) -> Self{
        Self{
            object,
            persistent,
            failure: false,
            category
        }
    }
}

impl TickerPackage for TickerEternal{
    fn call(self: Arc<Self>) -> bool{
        if self.persistent || Arc::weak_count(&self) > 1{
            self.object.repeat();
        }
        true
    }

    fn persistent(&self) -> bool{
        self.persistent
    }

    fn request(&self) -> u32 {0}

    fn delay(&self) -> u32 {0}

    fn set_request(&mut self, _request: u32) {}

    fn is_failure(&self) -> bool{
        self.failure
    }

    fn category(&self) -> usize{
        self.category
    }

    fn junk(self: Arc<Self>) -> bool{
        self.object.junk() || (Arc::weak_count(&self) == 0 && !self.persistent)
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct TickerVec{
    inner: Vec<Weak<dyn TickerPackage>>
}

impl TickerVec{
    pub fn new() -> Self{
        Self {inner: Vec::new()}
    }

    pub fn with_capacity(capacity: usize) -> Self{
        Self {inner: Vec::with_capacity(capacity)}
    }

    pub fn clean(&mut self){
        self.inner.drain_filter(|x| x.strong_count() == 0);
    }

    pub fn add(&mut self, value: Weak<dyn TickerPackage>){
        self.inner.push(value);
    }

    pub fn clear(&mut self){
        self.inner.clear();
    }

    pub fn len(&self) -> usize{
        self.inner.len()
    }

    pub fn reserve(&mut self, additional: usize){
        self.inner.reserve(additional);
    }
    pub fn capacity(&self) -> usize{
        self.inner.capacity()
    }

    pub fn shrink_to(&mut self, min_capacity: usize){
        self.inner.shrink_to(min_capacity);
    }

    pub fn is_empty(&self) -> bool{
        self.inner.is_empty()
    }
}

unsafe impl Send for TickerVec {}
unsafe impl Sync for TickerVec {}

#[repr(transparent)]
#[derive(Debug)]
pub struct TickerMonitor<T: PartialEq + Eq + Hash>{
    inner: HashMap<T, Weak<dyn TickerPackage>>
}

impl<T: PartialEq + Eq + Hash> TickerMonitor<T>{
    pub fn new() -> Self{
        Self {inner: HashMap::new()}
    }

    pub fn with_capacity(capacity: usize) -> Self{
        Self {inner: HashMap::with_capacity(capacity)}
    }

    pub fn clean(&mut self){
        self.inner.drain_filter(|_key, value| {value.strong_count() == 0});
    }

    pub fn add(&mut self, key: T, weak: Weak<dyn TickerPackage>) -> Option<Weak<dyn TickerPackage>>{
        self.inner.insert(key, weak)
    }

    pub fn regroup(&mut self, old: &T, new: T, overwrite: bool) -> bool{
        if overwrite{
            if let Some(weak) = self.inner.remove(old){
                self.inner.insert(new, weak);
            }
            true
        } else {
            if self.inner.get(&new).is_some(){
                false
            } else {
                if let Some(weak) = self.inner.remove(old){
                    self.inner.insert(new, weak);
                }
                true
            }
        }
    }

    pub fn contains(&self, key: &T) -> bool{
        self.inner.contains_key(key)
    }

    pub fn remove(&mut self, key: &T) -> Option<Weak<dyn TickerPackage>>{
        self.inner.remove(key)
    }

    pub fn clear(&mut self){
        self.inner.clear();
    }

    pub fn len(&self) -> usize{
        self.inner.len()
    }

    pub fn reserve(&mut self, additional: usize){
        self.inner.reserve(additional);
    }
    pub fn capacity(&self) -> usize{
        self.inner.capacity()
    }

    pub fn shrink_to(&mut self, min_capacity: usize){
        self.inner.shrink_to(min_capacity);
    }

    pub fn is_empty(&self) -> bool{
        self.inner.is_empty()
    }
}

impl<T: PartialEq + Eq + Hash + Clone> Clone for TickerMonitor<T>{
    fn clone(&self) -> Self {
        Self {inner: self.inner.clone()}
    }
}

unsafe impl<T: PartialEq + Eq + Hash> Send for TickerMonitor<T> {}
unsafe impl<T: PartialEq + Eq + Hash> Sync for TickerMonitor<T> {}

#[derive(Debug)]
pub struct TickerCaller<Args: Tuple, F: Fn<Args, Output = ()>>{
    function: *const F, 
    args: Args,
}

unsafe impl<Args: Tuple, F: Fn<Args, Output = ()>> Send for TickerCaller<Args, F> {}
unsafe impl<Args: Tuple, F: Fn<Args, Output = ()>> Sync for TickerCaller<Args, F> {}

impl<Args: Tuple, F: Fn<Args, Output = ()>> TickerCaller<Args, F>{
    pub fn new(function: *const F, args: Args) -> Box<Self>{
        Box::new(Self {function, args})
    }
}

impl<Args: Tuple, F: Fn<Args, Output = ()>> TickerObject for TickerCaller<Args, F> {
    fn call(self: Box<Self>) {
        unsafe{FnOnce::call_once(&*self.function, self.args)}
    }
}

#[derive(Debug)]
pub struct TickerOpener<Args: Tuple, T: ?Sized, F: Fn<Args, Output = ()>>{
    function: *const F,
    args: Args,
    object: Arc<Mutex<T>>
}

impl<Args: Tuple, T: ?Sized, F: Fn<Args, Output = ()>> TickerOpener<Args, T, F>{
    pub fn new(
        function: *const F,
        args: Args,
        object: Arc<Mutex<T>>
    ) -> Box<Self>{
        Box::new(
            Self{
                function,
                args,
                object
            }
        )
    }
}

impl<Args: Tuple, T: ?Sized, F: Fn<Args, Output = ()>> TickerObject for TickerOpener<Args, T, F>{
    fn call(self: Box<Self>) {
        unsafe {
            let a = self.object.lock();
            if a.is_ok(){
                /*let borrow = guard.deref_mut() as *mut T;
                let ptr = &self.args as *const Args;
                std::mem::transmute::<*const Args, *mut *mut T>(ptr).write(borrow);*/
                Fn::call(&*self.function, self.args);
            }
        }
    }
}

unsafe impl<Args: Tuple, T: ?Sized, F: Fn<Args, Output = ()>> Send for TickerOpener<Args, T, F> {}
unsafe impl<Args: Tuple, T: ?Sized, F: Fn<Args, Output = ()>> Sync for TickerOpener<Args, T, F> {}

#[derive(Debug)]
pub struct TickerRepeatCaller<Args: Tuple + Copy, F: Fn<Args, Output = ()>>{
    function: *const F, 
    args: Args
}

unsafe impl<Args: Tuple + Copy, F: Fn<Args, Output = ()>> Send for TickerRepeatCaller<Args, F> {}
unsafe impl<Args: Tuple + Copy, F: Fn<Args, Output = ()>> Sync for TickerRepeatCaller<Args, F> {}

impl<Args: Tuple + Copy, F: Fn<Args, Output = ()>> TickerRepeatCaller<Args, F>{
    pub fn new(function: *const F, args: Args) -> Box<Self>{
        Box::new(Self {function, args})
    }
}

impl<Args: Tuple + Copy, F: Fn<Args, Output = ()>> TickerObject for TickerRepeatCaller<Args, F> {
    fn call(self: Box<Self>) {
        unsafe{Fn::call(&*self.function, self.args)}
    }

    fn repeat(&self) {
        unsafe{Fn::call(&*self.function, self.args)}
    }

    fn junk(&self) -> bool {false}
}

#[derive(Debug)]
pub struct TickerRepeatOpener<Args: Tuple + Copy, T: ?Sized, F: Fn<Args, Output = ()>>{
    function: *const F,
    args: Args,
    object: Arc<Mutex<T>>
}

impl<Args: Tuple + Copy, T: ?Sized, F: Fn<Args, Output = ()>> TickerRepeatOpener<Args, T, F>{
    pub fn new(function: *const F, args: Args, object: Arc<Mutex<T>>) -> Box<Self>{
        Box::new(Self {
            function,
            args,
            object
        })
    }
}

impl<Args: Tuple + Copy, T: ?Sized, F: Fn<Args, Output = ()>> TickerObject for TickerRepeatOpener<Args, T, F>{
    fn call(self: Box<Self>) {
        unsafe {
            if self.object.lock().is_ok(){
                Fn::call(&*self.function, self.args);
            }
        }
    }

    fn repeat(&self) {
        unsafe {
            if self.object.lock().is_ok(){
                Fn::call(&*self.function, self.args);
            }
        }
    }

    fn junk(&self) -> bool {false}
}

unsafe impl<Args: Tuple + Copy, T: ?Sized, F: Fn<Args, Output = ()>> Send for TickerRepeatOpener<Args, T, F> {}
unsafe impl<Args: Tuple + Copy, T: ?Sized, F: Fn<Args, Output = ()>> Sync for TickerRepeatOpener<Args, T, F> {}

#[derive(Debug, Clone)]
pub struct TickerBasic{
    cycle: u64,
    tick: u32,
    stop: bool,
    timer: ZeroSleepTimer,
    safe_save: Vec<Weak<dyn TickerPackage>>,
    safe_save_state: bool,
    safe_save_index: u16,
    events: HashMap<u32, Vec<Arc<dyn TickerPackage>>>
}

impl TickerBasic{
    pub fn new(speed: u128, stop: bool) -> Self{
        Self{
            cycle: 0,
            tick: 0,
            stop,
            timer: ZeroSleepTimer::new(speed),
            safe_save: Vec::new(),
            safe_save_state: false,
            safe_save_index: 0,
            events: HashMap::new()
        }
    }

    fn cycle_up(&mut self, me: Arc<Mutex<Self>>){
        self.cycle += 1;
        self.add_event(
            Arc::new(TickerBox::new(
                Box::new(TickerCycler {ticker: me, new_cycle: false}), 
                1, 
                true,
                0
        )));
    }

    fn cycle_adder(&mut self, me: Arc<Mutex<Self>>){
        self.add_event(
            Arc::new(TickerBox::new(
                Box::new(TickerCycler {ticker: me, new_cycle: true}), 
                u32::MAX, 
                true,
                0
        )));
    }
}

impl Ticker for TickerBasic{
    fn add_event(&mut self, event: Arc<dyn TickerPackage>){
        let mut b = event.clone();
        unsafe {Arc::get_mut_unchecked(&mut b).set_request(self.tick);}
        match self.events.get_mut(&(event.delay() + self.tick)){
            Some(vec) => {
                vec.push(event)
            }
            None => {
                self.events.insert(event.delay() + self.tick, vec![event]);
            }
        }
    }

    fn tick(&mut self){
        if let Some(events) = self.events.remove(&self.tick){
            self.safe_save_state = true;
            self.safe_save_index = 0;
            self.safe_save = Vec::with_capacity(events.len());
            events.iter().for_each(|x| self.safe_save.push(Arc::downgrade(x)));
            for i in events{
                i.call();
                self.safe_save_index += 1;
            }
            self.safe_save_state = false;
        }
        self.tick += 1;
    }

    fn cycle(&mut self){
        loop{
            if self.stop{break;}
            self.tick();
            self.timer.wait();
        }
    }

    fn stop(&mut self){
        self.stop = true;
    }

    fn start(&mut self){
        self.stop = false;
    }

    fn get_speed(&self) -> u128{
        self.timer.target
    }

    fn set_speed(&mut self, speed: u128, reset: bool){
        self.timer.target = speed;
        if reset{
            self.timer.counter = 0;
            self.timer.now();
        }
    }

    fn get_tick(&self) -> (u64, u32){
        (self.cycle, self.tick)
    }

    fn set_tick(&mut self, cycle: u64, tick: u32){
        self.cycle = cycle;
        self.tick = tick;
    }

    fn get_sum_of_events(&self) -> usize{
        self.events.values().fold(0, |acc, v| acc + v.len())
    }

    fn get_autorun(&self) -> bool {
        self.stop
    }

    fn get_savestate_items(&self) -> &Vec<Weak<dyn TickerPackage>> {
        &self.safe_save
    }

    fn get_savestate(&self) -> bool {
        self.safe_save_state  
    }

    fn get_savestate_index(&self) -> usize {
        self.safe_save_index as usize
    }
    
    fn clear_poison(&mut self) {
        let weaks: Vec<Weak<dyn TickerPackage>> = self.safe_save.drain(..).collect();
        let new_events: Vec<Arc<dyn TickerPackage>> = weaks.into_iter()
            .map(|x| x.upgrade())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        for i in new_events{
            self.events.entry(self.tick + 1).or_insert(Vec::new()).push(i);
        }
    }
}

impl Display for TickerBasic{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, 
            "Cycle: {} Tick: {} Auto-run: {}\nNumber of events: {}",
            self.cycle, self.tick, self.stop, self.get_sum_of_events()
        )
    }
}

#[derive(Debug, Clone)]
pub struct TickerRepeater{
    cycle: u64,
    tick: u32,
    stop: bool,
    timer: ZeroSleepTimer,
    safe_save: Vec<Weak<dyn TickerPackage>>,
    safe_save_state: bool,
    safe_save_index: u16,
    events: Vec<Arc<dyn TickerPackage>>
}

impl TickerRepeater{
    pub fn new(speed: u128, stop: bool) -> Self{
        Self{
            cycle: 0,
            tick: 0,
            stop,
            timer: ZeroSleepTimer::new(speed),
            safe_save: Vec::new(),
            safe_save_state: false,
            safe_save_index: 0,
            events: Vec::new()
        }
    }
}

impl Ticker for TickerRepeater{
    fn add_event(&mut self, event: Arc<dyn TickerPackage>) {
        self.events.push(event)
    }

    fn tick(&mut self) {
        self.safe_save_state = true;
        self.safe_save_index = 0;
        self.safe_save = Vec::with_capacity(self.events.len());
        self.events.iter().for_each(|x| self.safe_save.push(Arc::downgrade(x)));
        for i in self.events.iter(){
            i.clone().call();
            self.safe_save_index += 1;
        }
        self.safe_save_state = false;
        self.tick += 1;
        if self.tick % REPEATER_JUNK == 0{
            self.events.drain_filter(|x| x.clone().junk());
        }
        if self.tick == 0{
            self.cycle += 1;
        }
    }
    
    fn cycle(&mut self){
        loop{
            if self.stop{break;}
            self.tick();
            self.timer.wait();
        }
    }

    fn get_speed(&self) -> u128{
        self.timer.target
    }

    fn set_speed(&mut self, speed: u128, reset: bool){
        self.timer.target = speed;
        if reset{
            self.timer.counter = 0;
            self.timer.now();
        }
    }

    fn get_tick(&self) -> (u64, u32){
        (self.cycle, self.tick)
    }

    fn set_tick(&mut self, cycle: u64, tick: u32){
        self.cycle = cycle;
        self.tick = tick;
    }

    fn get_sum_of_events(&self) -> usize{
        self.events.len()
    }

    fn get_autorun(&self) -> bool {
        self.stop
    }

    fn stop(&mut self){
        self.stop = true;
    }

    fn start(&mut self){
        self.stop = false;
    }
    
    fn get_savestate_items(&self) -> &Vec<Weak<dyn TickerPackage>> {
        &self.safe_save
    }

    fn get_savestate(&self) -> bool {
        self.safe_save_state  
    }

    fn get_savestate_index(&self) -> usize {
        self.safe_save_index as usize
    }

    fn clear_poison(&mut self) {
        let weaks: Vec<Weak<dyn TickerPackage>> = self.safe_save.drain(..).collect();
        let new_events: Vec<Arc<dyn TickerPackage>> = weaks.into_iter()
            .map(|x| x.upgrade())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        self.events = new_events;
    }
}

#[derive(Debug, Clone)]
pub struct TickerRepeaterAlt{
    cycle: u64,
    tick: u32,
    stop: bool,
    timer: ZeroSleepTimer,
    safe_save: Vec<Weak<dyn TickerPackage>>,
    safe_save_state: bool,
    safe_save_index: u16,
    events: Vec<Arc<dyn TickerPackage>>
}

impl TickerRepeaterAlt{
    pub fn new(speed: u128, stop: bool) -> Self{
        Self{
            cycle: 0,
            tick: 0,
            stop,
            timer: ZeroSleepTimer::new(speed),
            safe_save: Vec::new(),
            safe_save_state: false,
            safe_save_index: 0,
            events: Vec::new()
        }
    }
}

impl Ticker for TickerRepeaterAlt{
    fn add_event(&mut self, event: Arc<dyn TickerPackage>) {
        self.events.push(event)
    }

    fn tick(&mut self) {
        self.safe_save_state = true;
        self.safe_save_index = 0;
        self.safe_save = Vec::with_capacity(self.events.len());
        self.events.iter().for_each(|x| self.safe_save.push(Arc::downgrade(x)));
        self.events.retain(|x| {x.clone().call(); self.safe_save_index += 1; !x.clone().junk()});
        self.safe_save_state = false;
        self.tick += 1;
        if self.tick == 0{
            self.cycle += 1;
        }
    }
    
    fn cycle(&mut self){
        loop{
            if self.stop{break;}
            self.tick();
            self.timer.wait();
        }
    }

    fn get_speed(&self) -> u128{
        self.timer.target
    }

    fn set_speed(&mut self, speed: u128, reset: bool){
        self.timer.target = speed;
        if reset{
            self.timer.counter = 0;
            self.timer.now();
        }
    }

    fn get_tick(&self) -> (u64, u32){
        (self.cycle, self.tick)
    }

    fn set_tick(&mut self, cycle: u64, tick: u32){
        self.cycle = cycle;
        self.tick = tick;
    }

    fn get_sum_of_events(&self) -> usize{
        self.events.len()
    }

    fn get_autorun(&self) -> bool {
        self.stop
    }

    fn stop(&mut self){
        self.stop = true;
    }

    fn start(&mut self){
        self.stop = false;
    }

    fn get_savestate_items(&self) -> &Vec<Weak<dyn TickerPackage>> {
        &self.safe_save
    }

    fn get_savestate(&self) -> bool {
        self.safe_save_state  
    }

    fn get_savestate_index(&self) -> usize {
        self.safe_save_index as usize
    }

    fn clear_poison(&mut self) {
        let weaks: Vec<Weak<dyn TickerPackage>> = self.safe_save.drain(..).collect();
        let new_events: Vec<Arc<dyn TickerPackage>> = weaks.into_iter()
            .map(|x| x.upgrade())
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        self.events = new_events;
    }
}

pub trait Ticker: Send + Sync{
    fn add_event(&mut self, event: Arc<dyn TickerPackage>);
    fn stop(&mut self);
    fn start(&mut self);
    fn cycle(&mut self);
    fn tick(&mut self);
    fn get_speed(&self) -> u128;
    fn set_speed(&mut self, speed: u128, reset: bool);
    fn get_tick(&self) -> (u64, u32);
    fn set_tick(&mut self, cycle: u64, tick: u32);
    fn get_sum_of_events(&self) -> usize;
    fn get_autorun(&self) -> bool;
    fn get_savestate_items(&self) -> &Vec<Weak<dyn TickerPackage>>;
    fn get_savestate(&self) -> bool;
    fn get_savestate_index(&self) -> usize;
    fn clear_poison(&mut self);
}

impl Debug for dyn Ticker{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = self.get_tick();
        write!(f, "Cycle: {} Tick: {} Speed: {} Auto-run: {}\nNumber of events: {}", a.0, a.1, self.get_speed(), self.get_autorun(), self.get_sum_of_events())
    }
}

#[derive(Debug)]
enum WatcherType<T: ?Sized + 'static>{
    Mutex(Arc<Mutex<T>>),
    RwLock(Arc<RwLock<T>>)
}

impl<T: ?Sized + 'static> Clone for WatcherType<T>{
    fn clone(&self) -> Self {
        match self{
            WatcherType::Mutex(t) => WatcherType::Mutex(t.clone()),
            WatcherType::RwLock(t) => WatcherType::RwLock(t.clone())
        }
    }
}

#[derive(Debug)]
pub struct TickerDebugWatcher<T: ?Sized + 'static>{
    target: WatcherType<T>,
    watch_speed: u32,
    name: String
}

impl<T: ?Sized + 'static> TickerDebugWatcher<T>{
    pub fn new_mutex(target: Arc<Mutex<T>>, speed: u32, name: String) -> Self{
        Self{
            target: WatcherType::Mutex(target),
            watch_speed: speed,
            name
        }
    }

    pub fn new_rwlock(target: Arc<RwLock<T>>, speed: u32, name: String) -> Self{
        Self{
            target: WatcherType::RwLock(target),
            watch_speed: speed,
            name
        }
    }

    pub fn start(self, controller: Arc<Mutex<TickerController>>, category: usize){
        let obj = TickerCaller::new(&Self::watch, (self.clone(), controller.clone(), category));
        let package = Arc::new(TickerBox::new(obj, self.watch_speed, true, category)) as Arc<dyn TickerPackage>;
        controller.lock().unwrap().add_task(package);
    }

    fn watch(self, controller: Arc<Mutex<TickerController>>, category: usize){
        match &self.target{
            WatcherType::Mutex(target) => {
                if target.is_poisoned(){
                    println!("{} are poisoned!", self.name);
                }
            }
            WatcherType::RwLock(target) => {
                if target.is_poisoned(){
                    println!("{} are poisoned!", self.name);
                }
            }
        }
        let obj = TickerCaller::new(&Self::watch, (self.clone(), controller.clone(), category));
        let package = Arc::new(TickerBox::new(obj, self.watch_speed, true, category)) as Arc<dyn TickerPackage>;
        controller.lock().unwrap().add_task(package);
    }
}

impl<T: ?Sized + 'static> Clone for TickerDebugWatcher<T>{
    fn clone(&self) -> Self {
        Self{
            target: self.target.clone(),
            watch_speed: self.watch_speed,
            name: self.name.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct TickerControllerAccess{
    controller: Arc<Mutex<TickerController>>,
    tick: u32,
    cycle: u64
}

impl TickerControllerAccess{
    pub fn new(controller: Arc<Mutex<TickerController>>, tick: u32, cycle: u64) -> Self{
        Self{
            controller,
            tick,
            cycle
        }
    }

    pub fn tick(&self) -> u32{
        self.tick
    }

    pub fn cycle(&self) -> u64{
        self.cycle
    }

    pub fn add_task(&self, task: Arc<dyn TickerPackage>){
        self.controller.lock().unwrap().add_task(task);
    }
}

struct TickerCycler{
    ticker: Arc<Mutex<TickerBasic>>,
    new_cycle: bool
}

impl TickerObject for TickerCycler{
    fn call(self: Box<Self>) {
        unsafe {
            if self.new_cycle{
                Arc::get_mut_unchecked(&mut self.ticker.clone()).get_mut().unwrap().cycle_up(self.ticker);
            } else {
                Arc::get_mut_unchecked(&mut self.ticker.clone()).get_mut().unwrap().cycle_adder(self.ticker);
            }
        }
    }
}

#[derive(Debug)]
struct TickerInsider{
    thread: JoinHandle<()>,
    ticker: Arc<Mutex<dyn Ticker>>,
    stop: Arc<bool>,
    timer: f64,
    counter: Arc<Mutex<f64>>,
    inner_counter: f64,
    buffer: Arc<Mutex<Vec<Arc<dyn TickerPackage>>>>,
    checker: Arc<Mutex<u32>>,
    checker_counter: u32,
    checker_error_size: u32
}

