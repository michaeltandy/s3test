# S3Test

An in-memory fake S3, presented as a Java HTTP server you can point your S3 client at.

Written in core java, with no external dependencies (not even the AWS client or a logging framework).

[Travis build status](https://travis-ci.org/michaeltandy/s3test): ![Travis Continuous Integration build status image](https://api.travis-ci.org/michaeltandy/s3test.svg)

## You might not want to use this.

**I'm no longer really convinced this is a useful project.** It was never as fast as I hoped for, and a change to the AWS client which forces https which makes this library a lot less convenient to use. For these reasons, tests using this library may well make your codebase harder to understand and maintain, not easier. You should consider injecting a mock AmazonS3Client using a library like mockito.

## So how do I use it?

```
    //Create a HTTP s3 server
    S3Server instance = S3Server.createHttpServer();

    //Create a HTTPS s3 server
    S3Server httpsInstance = S3Server.createHttpsServer(keyStoreInputStream, keyStorePassword)

    instance.start();

    AmazonS3Client client = new AmazonS3Client(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
    client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    client.setEndpoint(instance.getAddress());
    
    // Perform some testing here!
    
    client.shutdown(); // You may want to put use a try-finally block so the
    instance.stop();   // server gets shut down even if an assertion fails.
```

To make your s3Client trust the certificate sent by s3Server, you will need to add the same keystore to your JVM by
adding following JVM parameters:

```
-Djavax.net.ssl.keyStore=<path to keystore.jks>
-Djavax.net.ssl.keyStorePassword=<password>
-Djavax.net.ssl.trustStore=<path to keystore.jks>
-Djavax.net.ssl.trustStorePassword=<password>
```


If constructed without InetSocketAddress specified, the server binds to localhost with a port chosen at random.
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
