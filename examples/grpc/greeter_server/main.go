//go:build example

/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/mrdomino/go-memoize"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

var (
	port = flag.Int("port", 50051, "The server port")

	memcacheAddr = flag.String("memcache_addr", "", "memcache server address")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
	memoSayHello memoize.Func[*pb.HelloRequest, *pb.HelloReply]
}

func newServer(cache memoize.Cache) *server {
	s := &server{}
	s.memoSayHello = memoize.Wrap(cache, func(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
		return s.SayHello_Raw(ctx, in)
	}, memoize.WithTTL(10*time.Minute), memoize.WithErrorFunc(func(err error) {
		log.Printf("ERROR: %v", err)
	}))
	return s
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return s.memoSayHello(ctx, in)
}

func (s *server) SayHello_Raw(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	flag.Parse()
	var cache memoize.Cache = memoize.NewLocalCache()
	if *memcacheAddr != "" {
		client := memcache.New(*memcacheAddr)
		if err := client.Ping(); err != nil {
			log.Fatalf("memcache.Ping(%q): %v", *memcacheAddr, err)
		}
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, newServer(cache))
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
