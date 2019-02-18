HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CC = gcc
CXX = g++

cov_comp = -fprofile-arcs -ftest-coverage -fno-inline
cov_lnk = -fprofile-arcs -ftest-coverage --coverage -fno-inline

NO_WARN = -w
TRIM_WARN = -Wno-unused-variable -Wno-unused-but-set-variable -Wformat-security
GDB_INFO = -g
CFLAGS = -DPOSIX -fpermissive -std=c++1z -L/usr/local/lib $(GDB_INFO)

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
ifeq ($(SYSTEM),Darwin)
LDFLAGS += `pkg-config --libs protobuf grpc++ grpc` -lgrpc++_reflection -ldl 
else
LDFLAGS += `pkg-config --libs protobuf grpc++ grpc` -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl
endif
CFLAGS_GRPC = -DGRPC_VERBOSITY=DEBUG -DGRPC_TRACE=all
LOG_LEVEL_NOTICE_MAJOR = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_GRPC_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST
LOG_LEVEL_TEST = -D_HIDE_HEARTBEAT_NOTICE -D_HIDE_NOEMPTY_REPEATED_APPENDENTRY_REQUEST -D_HIDE_GRPC_NOTICE # -D_HIDE_RAFT_DEBUG
LOG_LEVEL_LIB = $(LOG_LEVEL_NOTICE_MAJOR) -D_HIDE_RAFT_DEBUG -D_HIDE_DEBUG -D_HIDE_TEST_DEBUG

OBJ_EXT=o

ROOT = .
# Important not to include ".", or gcov -r will fail with some files
SRC_ROOT = src
BIN_ROOT = bin
OBJ_ROOT = $(BIN_ROOT)/obj

SRCS = $(wildcard $(SRC_ROOT)/*.cpp)
OBJS = $(patsubst $(SRC_ROOT)%, $(OBJ_ROOT)%, $(patsubst %cpp, %o, $(SRCS)))

all: test

test: $(SRCS) $(SRC_ROOT)/test/test.cpp
	$(CXX) $(CFLAGS) $(LDFLAGS) -Isrc/ $(SRCS) $(SRC_ROOT)/test/test.cpp -o $(ROOT)/test -lpthread /usr/local/lib/libgtest.a /usr/local/lib/libhiredis.a 

kv: /usr/local/lib/libnuft.a
	$(CXX) $(CFLAGS) $(LOG_LEVEL_LIB) $(SRC_ROOT)/test/kv.cpp -o $(ROOT)/kv -pthread /usr/local/lib/libnuft.a $(LDFLAGS) 

$(OBJ_ROOT):
	mkdir -p $(OBJ_ROOT)


.PHONY: clean
clean: clc
	rm -rf $(BIN_ROOT)
	rm -f core
	rm -rf ./test

.PHONY: clc
clc:
	rm -f *.err
	rm -f *.out
