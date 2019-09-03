fn main() {
    for i in 1 .. 1_000_000 {
        let j = j%1000;
        let k = j%10;
        let l = (j+500) * 3;
        println!("{},{},{},{}", i,j,k,l);
    }
}