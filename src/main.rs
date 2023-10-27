use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").expect("failed to bind");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {:?}", stream);
            }
            Err(e) => {
                println!("Failed to accept connection: {}", e);
            }
        }
    }
}
