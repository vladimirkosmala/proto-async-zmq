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
      client_socket_(ctx_, ZMQ_DEALER)
  {}

  void start() {
    // generate random identity
    char identity[10] = {};
    sprintf(identity, "%04X-%04X", within(0x10000), within(0x10000));
    std::cout << "  start client '" << identity << "'" << std::endl;
    client_socket_.setsockopt(ZMQ_IDENTITY, identity, strlen(identity));
    client_socket_.connect("tcp://localhost:5570");

    zmq::pollitem_t items[] = {client_socket_, 0, ZMQ_POLLIN, 0};
    int request_nbr = 0;
    try {
      while (true) {
        s_sleep(1000);
        char request_string[16] = {};
        sprintf(request_string, "agent/obj%d", ++request_nbr);
        s_send(client_socket_, request_string);
        std::string message = s_recv(client_socket_);
        std::cout << "CLIENT: received response '" << message << "'" << std::endl;
      }
    }
    catch (std::exception &e) {}
  }

private:
  zmq::context_t ctx_;
  zmq::socket_t client_socket_;
};

//  Each worker task works on one request at a time and sends a random number
//  of replies back, with random delays between replies:

const std::string space(" ");
const std::string code200("200");
const std::string code404("404");
const std::string code500("500");

class server_worker {
public:
  server_worker(zmq::context_t &ctx)
    : ctx_(ctx),
      worker_socket_(ctx_, ZMQ_DEALER)
  {}

  void work() {
    worker_socket_.connect("inproc://backend");

    try {
      while (true) {
        std::string identity = s_recv(worker_socket_);
        std::string message = s_recv(worker_socket_);

        std::cout << "WORKER: received request '" << message << "' from '" << identity << "'" << std::endl;

        s_sleep(within(1000) + 1);

        s_sendmore(worker_socket_, identity);
        switch (within(3)) {
          case 0:
            s_send(worker_socket_, message + space + code200 + space + std::string("{\"ok\":1}"));
            break;
          case 1:
            s_send(worker_socket_, message + space + code404);
            break;
          case 2:
            s_send(worker_socket_, message + space + code500 + space + std::string("Unable to convert"));
            break;
        }
      }
    }
    catch (std::exception &e) {}
  }

private:
  zmq::context_t &ctx_;
  zmq::socket_t worker_socket_;
};

//  This is our server task.
//  It uses the multithreaded server model to deal requests out to a pool
//  of workers and route replies back to clients. One worker can handle
//  one request at a time but one client can talk to multiple workers at
//  once.

class server_task {
public:
  server_task()
    : ctx_(1),
      frontend_(ctx_, ZMQ_ROUTER),
      backend_(ctx_, ZMQ_DEALER)
  {}

  enum { kMaxThread = 5 };

  void run() {
    std::cout << "server binding frontend and backend" << std::endl;
    frontend_.bind("tcp://127.0.0.1:5570");
    backend_.bind("inproc://backend");

    std::cout << "server deploying threads and workers..." << std::endl;
    for (int i = 0; i < kMaxThread; ++i) {
      printf("  start worker %d\n", i);
      server_worker *worker = new server_worker(ctx_);

      std::thread *thread = new std::thread(std::bind(&server_worker::work, worker));
      thread->detach();
    }

    zmq::proxy(frontend_, backend_, nullptr);
  }

private:
  zmq::context_t ctx_;
  zmq::socket_t frontend_;
  zmq::socket_t backend_;
};

//  The main thread simply starts several clients and a server, and then
//  waits for the server to finish.

int main(void)
{
  std::cout << "Instanciating..." << std::endl;
  client_task ct1;
  client_task ct2;
  client_task ct3;
  server_task st;

  std::cout << "Starting threads..." << std::endl;
  std::thread t1(std::bind(&client_task::start, &ct1));
  usleep(1000);
  std::thread t2(std::bind(&client_task::start, &ct2));
  usleep(1000);
  std::thread t3(std::bind(&client_task::start, &ct3));
  usleep(1000);
  std::thread t4(std::bind(&server_task::run, &st));

  t1.detach();
  t2.detach();
  t3.detach();
  t4.detach();

  getchar();

  return 0;
}
