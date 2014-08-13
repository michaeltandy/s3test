# S3Test

An in-memory fake S3, presented as a Java HTTP server you can point your S3 client at.

Written in core java, with no external dependencies (not even the AWS client or a logging framework).

## So how do I use it?

```
    S3Server instance = new S3Server();
    instance.start();

    AmazonS3Client client = new AmazonS3Client(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
    client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    client.setEndpoint(instance.getAddress());
    
    // Perform some testing here!
    
    client.shutdown(); // You may want to put use a try-finally block so the
    instance.stop();   // server gets shut down even if an assertion fails.
```

If constructed without any parameters, the server binds to a port chosen at random.
This means you can run several tests in parallel and they'll each have their own independent S3Server.

## Sounds good. So what doesn't it do?

Missing features include:
* No authentication
* No support for tagging or ACLs
* No support for multipart uploads
* No support for POST, HEAD, OPTIONS
* No support for if-modified-since and headers like that
* No support for torrents
* If you forget to stop it, after a few minutes (or seconds) it should complain instead of just making your tests hang.
* Refactor so users can extend the server to configure the logging
* Refactor so the HTTP server implementation is pluggable?
* Some sort of developer-friendly syntax for making assertions about stored data?

## What license is it under?

This project is (c) Michael Tandy
it's released under the [MIT license](http://en.wikipedia.org/wiki/MIT_License).