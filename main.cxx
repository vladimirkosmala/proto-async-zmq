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

class ClientTask {
public:
  ClientTask()
    : mCtx(1),
      mClientSocket(mCtx, ZMQ_DEALER),
      mThread(std::bind(&ClientTask::start, this))
  {}

  ~ClientTask() {
    mRun = false;
    std::cout << "ending client" << std::endl;
    // mClientSocket.~socket_t();
    // mCtx.~context_t();
    // mThread.join();
  }

  void start() {
    mRun = true;
    // generate random identity
    char identity[10] = {};
    sprintf(identity, "%04X-%04X", within(0x10000), within(0x10000));
    std::cout << "  start client '" << identity << "'" << std::endl;
    mClientSocket.setsockopt(ZMQ_IDENTITY, identity, strlen(identity));
    mClientSocket.setsockopt(ZMQ_RCVTIMEO, 2000);
    mClientSocket.setsockopt(ZMQ_SNDTIMEO, 2000);
    mClientSocket.connect("tcp://localhost:5570");

    zmq::pollitem_t items[] = {mClientSocket, 0, ZMQ_POLLIN, 0};
    int requestNbr = 0;
    try {
      while (true) {
        if (!mRun) {
          return;
        }

        s_sleep(1000);

        if (!mRun) {
          return;
        }

        char requestString[16] = {};
        sprintf(requestString, "agent/obj%d", ++requestNbr);
        s_send(mClientSocket, requestString);
        std::string message = s_recv(mClientSocket);
        std::cout << "CLIENT " << identity << ": received response '" << message << "'" << std::endl;
      }
    }
    catch (std::exception &e) {
      std::cout << e.what() << std::endl;
    }
  }

private:
  zmq::context_t mCtx;
  zmq::socket_t mClientSocket;
  std::thread mThread;
  bool mRun;
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

class ServerWorker {
public:
  ServerWorker(zmq::context_t &ctx)
    : mCtx(ctx),
      mWorkerSocket(mCtx, ZMQ_DEALER),
      mThread(std::bind(&ServerWorker::start, this))
  {}

  ~ServerWorker() {
    mRun = false;
    std::cout << "ending worker" << std::endl;
    mThread.join();
  }

  void start() {
    mRun = true;
    mWorkerSocket.setsockopt(ZMQ_RCVTIMEO, 200);
    mWorkerSocket.connect("inproc://backend");

    try {
      while (true) {
        if (!mRun) {
          return;
        }

        std::string identity = socketReceive();
        if (identity.empty()) {
          // empty or timeout
          continue;
        }

        std::string payload = socketReceive();
        if (payload.empty()) {
          // empty or timeout
          continue;
        }

        std::cout << "WORKER: received request '" << payload << "' from '" << identity << "'" << std::endl;

        std::string response = handleRequest(payload);

        // always success to send because of inproc communication
        socketSend(identity, response);
      }
    }
    catch (std::exception &e) {
      // Context was terminated
    }
  }

  std::string handleRequest(std::string request) {
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
  std::string socketReceive() {
    zmq::message_t message;
    bool received = mWorkerSocket.recv(&message);
    if (!received) {
      // timeout
      return empty;
    }
    return std::string(static_cast<char*>(message.data()), message.size());
  }

  //  Sends string as 0MQ string, as multipart non-terminal
  bool socketSend(const std::string & identity, const std::string & payload) {
    zmq::message_t identityMessage(identity.size());
    memcpy(identityMessage.data(), identity.data(), identity.size());

    mWorkerSocket.send(identityMessage, ZMQ_SNDMORE);

    zmq::message_t payloadMessage(payload.size());
    memcpy(payloadMessage.data(), payload.data(), payload.size());

    return mWorkerSocket.send(payloadMessage);
  }

private:
  zmq::context_t &mCtx;
  zmq::socket_t mWorkerSocket;
  std::thread mThread;
  bool mRun;
};

//  This is our server task.
//  It uses the multithreaded server model to deal requests out to a pool
//  of workers and route replies back to clients. One worker can handle
//  one request at a time but one client can talk to multiple workers at
//  once.

class ServerTask {
public:
  ServerTask(uint8_t num_threads)
    : mCtx(1),
      mNumThreads(num_threads),
      mFrontend(mCtx, ZMQ_ROUTER),
      mBackend(mCtx, ZMQ_DEALER),
      mThread(std::bind(&ServerTask::start, this))
  {}

  ~ServerTask() {
    std::cout << "ending server task" << std::endl;
    mFrontend.~socket_t();
    mBackend.~socket_t();
    mCtx.~context_t();
    mThread.join();
  }

  void start() {
    std::cout << "server binding frontend and backend" << std::endl;
    mFrontend.bind("tcp://127.0.0.1:5570");
    mBackend.bind("inproc://backend");
    std::vector<ServerWorker *> workers;

    std::cout << "server deploying threads and workers..." << std::endl;
    for (int i = 0; i < mNumThreads; ++i) {
      printf("  start worker %d\n", i);
      ServerWorker *worker = new ServerWorker(mCtx);
      workers.push_back(worker);
    }

    try {
      zmq::proxy(mFrontend, mBackend, nullptr);
    }
    catch (std::exception &e) {}

    for (int i = 0; i < mNumThreads; ++i) {
      puts("deleting workers");
      delete workers[i];
    }
  }

private:
  zmq::context_t mCtx;
  zmq::socket_t mFrontend;
  zmq::socket_t mBackend;
  std::thread mThread;
  uint8_t mNumThreads;
};

//  The main thread simply starts several clients and a server, and then
//  waits for the server to finish.

int main(void)
{
  std::cout << "Starting..." << std::endl;

  {
    ClientTask clientTask1;
    ClientTask clientTask2;
    ClientTask clientTask3;
    ServerTask serverTask(5);

    getchar();
  }

  return 0;
}
