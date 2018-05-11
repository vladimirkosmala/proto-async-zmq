//  Asynchronous client-to-server (DEALER to ROUTER)
//
//  While this example runs in a single process, that is to make
//  it easier to start and stop the example. Each task has its own
//  context and conceptually acts as a separate process.

#include <vector>
#include <thread>
#include <memory>
#include <functional>

#include <zmq.hpp>
#include "zhelpers.hpp"

//  This is our client task class.
//  It connects to the server, and then sends a request once per second
//  It collects responses as they arrive, and it prints them out. We will
//  run several client tasks in parallel, each with a different random ID.
//  Attention! -- this random work well only on linux.

class client_task {
public:
  client_task()
    : ctx_(1),
      client_socket_(ctx_, ZMQ_DEALER),
      thread_(std::bind(&client_task::start, this))
  {}

  ~client_task() {
    run_ = false;
    std::cout << "ending client" << std::endl;
    // client_socket_.~socket_t();
    // ctx_.~context_t();
    // thread_.join();
  }

  void start() {
    run_ = true;
    // generate random identity
    char identity[10] = {};
    sprintf(identity, "%04X-%04X", within(0x10000), within(0x10000));
    std::cout << "  start client '" << identity << "'" << std::endl;
    client_socket_.setsockopt(ZMQ_IDENTITY, identity, strlen(identity));
    client_socket_.setsockopt(ZMQ_RCVTIMEO, 2000);
    client_socket_.setsockopt(ZMQ_SNDTIMEO, 2000);
    client_socket_.connect("tcp://localhost:5570");

    zmq::pollitem_t items[] = {client_socket_, 0, ZMQ_POLLIN, 0};
    int request_nbr = 0;
    try {
      while (true) {
        if (!run_) {
          return;
        }

        s_sleep(1000);

        if (!run_) {
          return;
        }

        char request_string[16] = {};
        sprintf(request_string, "agent/obj%d", ++request_nbr);
        s_send(client_socket_, request_string);
        std::string message = s_recv(client_socket_);
        std::cout << "CLIENT " << identity << ": received response '" << message << "'" << std::endl;
      }
    }
    catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
  }

private:
  zmq::context_t ctx_;
  zmq::socket_t client_socket_;
  std::thread thread_;
  bool run_;
};

//  Each worker task works on one request at a time and sends a random number
//  of replies back, with random delays between replies:
// ZMQ C++ doc: http://api.zeromq.org/4-2:zmq-setsockopt#toc36
// ZMQ options: http://api.zeromq.org/2-1:zmq-cpp

const std::string empty("");
const std::string space(" ");
const std::string code200("200");
const std::string code404("404");
const std::string code500("500");

class server_worker {
public:
  server_worker(zmq::context_t &ctx)
    : ctx_(ctx),
      worker_socket_(ctx_, ZMQ_DEALER),
      thread_(std::bind(&server_worker::start, this))
  {}

  ~server_worker() {
    run_ = false;
    std::cout << "ending worker" << std::endl;
    thread_.join();
  }

  void start() {
    run_ = true;
    worker_socket_.setsockopt(ZMQ_RCVTIMEO, 200);
    worker_socket_.connect("inproc://backend");

    try {
      while (true) {
        if (!run_) {
          return;
        }

        std::string identity = s_recv(worker_socket_);
        if (identity.empty()) {
          // empty or timeout
          continue;
        }

        std::string payload = s_recv(worker_socket_);
        if (payload.empty()) {
          // empty or timeout
          continue;
        }

        std::cout << "WORKER: received request '" << payload << "' from '" << identity << "'" << std::endl;

        std::string response = handle_request(payload);

        // always success to send because of inproc communication
        s_sendmore(worker_socket_, identity);
        s_send(worker_socket_, response);
      }
    }
    catch (std::exception &e) {
      // Context was terminated
    }
  }

  std::string handle_request(std::string request) {
    s_sleep(within(1000) + 1);
    switch (within(3)) {
      case 0:
        return request + space + code200 + space + std::string("{\"ok\":1}");
        break;
      case 1:
        return request + space + code404;
        break;
      default:
      case 2:
        return request + space + code500 + space + std::string("Unable to convert");
        break;
    }
  }

  //  Receive 0MQ string from socket and convert into string
  static std::string s_recv(zmq::socket_t & socket) {
    zmq::message_t message;
    bool received = socket.recv(&message);
    if (!received) {
      // timeout
      return empty;
    }
    return std::string(static_cast<char*>(message.data()), message.size());
  }

  //  Sends string as 0MQ string, as multipart non-terminal
  static bool s_sendmore(zmq::socket_t & socket, const std::string & string) {
    zmq::message_t message(string.size());
    memcpy(message.data(), string.data(), string.size());

    return socket.send(message, ZMQ_SNDMORE);
  }

  //  Convert string to 0MQ string and send to socket
  static bool s_send(zmq::socket_t & socket, const std::string & string) {
    zmq::message_t message(string.size());
    memcpy(message.data(), string.data(), string.size());

    return socket.send(message);
  }

private:
  zmq::context_t &ctx_;
  zmq::socket_t worker_socket_;
  std::thread thread_;
  bool run_;
};

//  This is our server task.
//  It uses the multithreaded server model to deal requests out to a pool
//  of workers and route replies back to clients. One worker can handle
//  one request at a time but one client can talk to multiple workers at
//  once.

class server_task {
public:
  server_task(uint8_t num_threads)
    : ctx_(1),
      num_threads_(num_threads),
      frontend_(ctx_, ZMQ_ROUTER),
      backend_(ctx_, ZMQ_DEALER),
      thread_(std::bind(&server_task::start, this))
  {}

  ~server_task() {
    std::cout << "ending server task" << std::endl;
    frontend_.~socket_t();
    backend_.~socket_t();
    ctx_.~context_t();
    thread_.join();
  }

  void start() {
    std::cout << "server binding frontend and backend" << std::endl;
    frontend_.bind("tcp://127.0.0.1:5570");
    backend_.bind("inproc://backend");
    std::vector<server_worker *> workers;

    std::cout << "server deploying threads and workers..." << std::endl;
    for (int i = 0; i < num_threads_; ++i) {
      printf("  start worker %d\n", i);
      server_worker *worker = new server_worker(ctx_);
      workers.push_back(worker);
    }

    try {
      zmq::proxy(frontend_, backend_, nullptr);
    }
    catch (std::exception &e) {}

    for (int i = 0; i < num_threads_; ++i) {
      puts("deleting workers");
      delete workers[i];
    }
  }

private:
  zmq::context_t ctx_;
  zmq::socket_t frontend_;
  zmq::socket_t backend_;
  std::thread thread_;
  uint8_t num_threads_;
};

//  The main thread simply starts several clients and a server, and then
//  waits for the server to finish.

int main(void)
{
  std::cout << "Starting..." << std::endl;

  {
    client_task ct1;
    client_task ct2;
    client_task ct3;
    server_task st(5);

    getchar();
  }

  return 0;
}
