
package uk.me.mjt.s3test;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

public class MultipleRequestTest {

    private static final int S3_SERVER_PORT = 27903;
    private static final int S3_SERVER_PORT_HTTPS = 27904;

    private static final String BUCKET = "application-bucket";
    private static final String KEY = "solutions-folder";

    private static S3Server s3Server;
    private static S3Server s3ServerHttps;
    private static AmazonS3Client amazonS3;
    private static AmazonS3Client amazonS3Https;

    @BeforeClass
    public static void setUpTestCase() throws Exception {
        setUpS3Server();
        setUpS3ServerHttps();
    }

    @AfterClass
    public static void tearDownTestCase() throws Exception {
        s3Server.stop();
    }

    @Test
    public void wot() throws Exception {
        // User on Windows reported:
        //Client: Writing data with length 5188
        //Server: Stored object length 4380 with MD5 9204D1FF963B860911C10E4EAEF7CD42
        //Client: com.amazonaws.AmazonClientException: Unable to verify integrity of data upload.  Client calculated content hash (contentMD5: DXWKRj0pTt3pEBfDvwwg8A== in base 64) didn't match hash (etag: 9204D1FF963B860911C10E4EAEF7CD42 in hex) calculated by Amazon S3.  You may need to delete the data stored in Amazon S3. (metadata.contentMD5: null, md5DigestStream: com.amazonaws.services.s3.internal.MD5DigestCalculatingInputStream@77d680e6, bucketName: buckett, key: key)

        byte[] byteContent = DatatypeConverter.parseBase64Binary(
            "LS0tLS1CRUdJTiBDTVMtLS0tLQ0KTUlBR0NTcUdTSWIzRFFFSEE2Q0FNSUFDQVFJeE1LSXVB"
                + "Z0VFTUFJRUFEQUxCZ2xnaGtnQlpRTUVBUVVFR0ZsQw0KWWJOdVpxTG5oSWkzbVRsZm9Dem05bFpmWCtuOHN6Q0FCZ2txaGtpRzl3MEJ"
                + "Cd0V3SFFZSllJWklBV1VEQkFFQw0KQkJEREZCdDJ0dDNQT2ZMNXgybHduUzJab0lBRWdnUG9yaXFKMmNKMzJVWVFDWjkwSlRVRTByWH"
                + "paVFhSNFp5cg0KYVBWZmFvRUEzeFoxZERnUDRBY1BCL1gzT2l0c3JBeU5nVkJZUnNlS28rSE9JUVY3VkFEazlzaWVWbWdSRnI4SQ0Ka"
                + "GswcDJLbnJFZnJWOVJ0UEw1aDJnYzRoNGt1SituZHJ2VE9waTMyQW8vSG1WYnd0UHl1VnNqTERsRk1FU2pZVg0KZzFiVFp5WWJGWktj"
                + "bVcreEJudXY2N1dMVTYra0E5RnpZbWl4dGg3aHVtSWNVNmJRZk1NWmpNb2xaV3pXdDBTbg0KUEFqNWIyQkt1WENsVkxIeUxSdU9NaUx"
                + "BaXJscjY3eVFVUkFoSHN6Y3VUd1J0MFdqcUxOSE5tVHNNcU5jTFl1QQ0KUDAxQVB2ZytJMlFmTFJVRDQwQ1FKcTZIUlF1ZW1kc1hPZF"
                + "VzajcxamRlUjNJR0xQWlc1VmE4UE90QSs4V1Qyag0KaWFKTHlIZkN2Q0wyL2NXTWsyVmNRcjZyZWVPVUYvcS84cUQyZjNkS3Z0WHFlM"
                + "zBFSU9SM0czYWVjVWVGdlBVcw0KWi9JajdOWEUweGxUUThRdjM2c0NKd0RQWllvMzdpK1dNT1RRVHNLaTV1SFlWd0h0bC8zc0ZNbUw2"
                + "S3hhSzJ5Vg0KaE1JSE9Za2RrK3Z6RFZpV2c4Nmt1YlA1c2ZjQTRZK1oxNFQwOWllYW5ST2VxekI3U0M2djI1MGlFTzdKSitKbg0KK1M"
                + "wQzJpTUtjVG53dzdwSGYvNmYrRkExemlURU9Jc2NJZzhJYjNMQXpqVjBPYXhLTzZpNklnUi9BdEtsdEJFbw0KaG51SlFDeWtqVnA1en"
                + "BXdDBlRFJUUWE2SWhqYmY2ZkROdzQzSG1PWm9aM05YZS9SN2NzUklmVFhzemlHQUdHQg0KZmpMUFBDallkdVE3ZElpU3VZVlNzSzZEd"
                + "ldZeDlwQXdodFRUM1QzdjYyTmpCZExzY01TbDhRYnR2SHFXaFRKNQ0KSTI0Sm11bUZ0OWxDMVd2RTZiU2txNzIvYVUrZk5tdEtURG81"
                + "dHJ4MGJGbEFoRlhOYkZvZFdLZHBicEpRYTZXOQ0KejVlZWZVbVRhbUJMaldWMlpTNGhWTFpPczF0UTBFc2dISVZzOTZGUmtabUJ4Tzl"
                + "WTERlR09iMlZ5Ky9HMWIwNQ0KM1dsVnFTZ0dVNDRsMTc5YWl1dUYzVVV4MmRaNnBKMEF1THRobStYTC9MWWt4WWd1QjJPZ05jOE96Nm"
                + "p6VGFYcA0KZk0va0pXdms5WEllTVNSVGxCOFNKdm04bXlFRzYzTWo3bXp2NGZ2amhuS3pFaHg4SDd6SE1EczlnaE4vVTV6ag0KTXF1Z"
                + "DBqaXNlWXR6YWw4U1pvdHpLNklRWHZFR0RMTVVUUUVySzZwMmpzeDNvRXo0OExiL0VDd3BHSjFaNERwNA0KVnptUVlNcVkvUHJmVlBz"
                + "QTRIWWxhZEQ1ejV2QjNwSm5CWUtrUkxLSTNPVWxReTkyNkxsZUhSU1ZTKytQdks4dg0KZFBXUDJGa1ZORkNiU1hsT3BOZzNmNXA4TkF"
                + "UTGlQMGp4SjlkQVd5NS9MMUVTQ3FyZng3Vkt3emp5SWFQL0xmbg0KZCtvK1ZtaFJLTzhzMG50UFZ6Z2YxY29HT2ExRyt4NVVIa0c3Sm"
                + "1ENWMvMlJqWWFWTlJxNGdCeEdRbFZ6UitrQg0KVEQ1d21EOFNnYk4yNFFjYWl6d2pQaXh1ZUJXZU9ycW56VnA0N3k2b2ZjYmF6SnZmY"
                + "zBydWxkMVVrV1A0ZXNVMw0KWFVRcFZWemlqUUNnZGQ4NHZ4aFM5d1NDQStpbGZuekNJdXNZUHJ1ZDFiMHRVY3drclhydysycjUyWlpr"
                + "a2lDKw0Kb3kxQTZsdHdORkpLQ1JtcHBpTmc5VkMzQjNsS1l0SzRDb2hmV1kxeVpjbVgwaGIySWpMMzZubXdvS3ZzTlczZw0KR1ZPaHh"
                + "TMlUwK1I2YStZY0cvL1JMV1JycDg0c2ZMNVVQUEhQd01DRmZOeDFRS25XdEtPREdSNFpVVjNnRzlhUg0KL3ErQmhZRzdFS0c3c0lIUX"
                + "crL3h3eWN1NjZlcGFjU1phYmx0clJVWTkxenZMK3BuQ2VqQXNYWFdZSFNxbkp4Yg0KYkpGTEFLSkVwSDUraGRoYUs4T1RyNzI5Y3dpL"
                + "0pwdTRjUUQ5ZGtPTHVNVFdVQjl2NElOdzZPc25ULzE1WUpwcw0KcHdyeXZmZnRCYlU1SFF5V0p5RmNDY2RjYjI5ejZ2c2szTEhzUUcz"
                + "emlsNzNaTDhnZ3dRV3pWNVltZENrME9GKw0Kc1NUL2ZJdy9CQnBORW1OMUQ4RUx5cFRpS2dpV2t6emU5WnpIVXIwNmxBWCtnQnk2elp"
                + "wczhXNEpvV09reURUag0KcEtlaHA1YkJkRXdZaE9kNjdqQ3hhN0F4cVZzSHBQbk1qWHhDc1Jidy9VWS9YbGpmSTZrNkwyeGk2d09WRl"
                + "NucQ0KN1oxREVCOE1RcmVNd2taeVFYZGxvcUp0dlZoU3Z4YndvUXovd3lrOEltWW1Zem96aVhiNlBqaktaejhjQ2VUNQ0KWjF1ZGQxS"
                + "2pkbEJFYVluNFU0R1BMek5jMGhvVWZCdXliLzdmS3YwUnE5eHRhM0hVZHZ3QUVvWThIZnJ3aTNBQg0KZmUvbGMvQUVVQ3dlbzVWZmZv"
                + "TUZNZFROYnJLckVCbFNtbGdmVU95bDRRc3dxMndZREdZTG5FaCtRWHlVMG9JMg0KVkxIVUpDNCtFWFBLc1MwRzhGbFJhV2NkMjIzTkN"
                + "VUGVOdFpPTDltWXIxeFFiNXFqMzJLcnZicW9Zb2ptOXNHeg0Kc0lOK1phTE00ZzUyVUZyaisxbEkxU2J3MmxzbEJFSlhRcDNtMU5FNz"
                + "U5ZzNiUjhtRHRrZ215NUd3QWcvTmhDUg0KbUNYWUpRWEhMWEJlVWNmNkloV05NOEJTQkdFbk5IeUZWbVZubDJJR25iNTN6YnF4dW5Kb"
                + "VNNQjJzUit6cFBvSA0KblhDWHdVMjVSOHJXSTgxUWs4UUpRcWlGQkVKKzN5ZE10MDdBZGVCbXRUaWxHVGhEUFBEb3ZyUC9CM2dRZjEy"
                + "Mw0KNnR5NC9HOExTdVRPM0pnQ3lwdmpvNkh3bi94am1jMEpzcktjRStZTndLVmhZc1gxTGdwUWFRRlk1V0M5UUh6Lw0KbWxoL2VDRkY"
                + "2WTJHSk5oL3ZsWEloeFRDeE5GSFZ0MENPVCsxSEJmb3BwbzFlQVJyZVNBaGpxSDFORGpKL3R0dg0Kd0orcnpoK1dhR1djcDlBdWxUQ1"
                + "gzWnJlRDkyNG1tNGVDdmdRTGhIRE9rMU5DNmxWRFJOcU1UUnFqUm9INit1OA0KN3F5WU0vQ1l4b0pseHZjaVVXRm1QK0RwM0ZsRnk4V"
                + "3FVT3AvK3Y1anVyRkRzcFpTRmczVVNiLzNybnVSNkFPUw0Kd0J2UDVhS3p4dnZDcnZWVXZNVC9oQnVESy9hODJBc0lUVEVYa0MrNFhH"
                + "S3FaY24xMGQyUDk5b0ZqNXpuS041Mw0KVHdMdzlHQ013c1VVb0tyMzZ6NncvcGZYbFYyWEwwWndiTVdjeFJjaG5FSFZ1bDlPRFdjVGJ"
                + "5dDF3aGx0OHROWA0KTUhwZkYvaFgveE5DbkJoY0JJSUQ2TG9INm5SVXgwZzBKWXRqcDFFMEE1WCtVNW45dnZWUmNMbWpwdlEwUmUrTA"
                + "0KZE8zVVhJZnZ2YS9zNkkxRFY5ZU9VUkliMzJOTno1RFZiVWMzY1EwdTNNRTZlNjcwQXJLSGc3WHY5L3FCQW1qYg0KK210NEgxTlh4e"
                + "nlTUHRCdXpjQ2FMTTNrNHdzMy84SVZVdkZOMURvd3JTUnN5Z1BOUTNQL2JoNUdiWTYxSzltLw0KbGM1TkdlV0lLUzFPS3JheUVPQzBD"
                + "Rkw0YWtnQ3ZaQlpqc1JOeUVGcFpIY0FlS3l4QlFZUHVLK2M0RlQ0WWFsdw0KZzZwT21qZUVjUnNEanhrbHpUWGkyaDNNT1dISnp2UVJ"
                + "6UmQ4ajZjTFZVMTVhSVZjbXNnVTNOTmVKU0NQRERoNw0KNEVmZm5Na2xSZ3hqZWNIbm81em92QTIxTFdWMHE3ajlsWTV3TnlMb0t6NW"
                + "QxQ3FwaUNPYVlhRlFPbHRnNCtZYQ0KUU1oNTVOZC9UdnA1SFlGS2QrcktsVE9yTHJhbjVhZE44L1g1dnoveUNZVS9mdVZUczhLeWNmQ"
                + "21JOWRZeStMUQ0KN0ZBY1lRNkl0d1ZPVXBQR1dGdTBKdDlIaGRkZDJZZVplYWVYaSsvMk8vMWxHMlp4TU12eUw1NUl6Vm4yZXJJbw0K"
                + "bmx1c0E1eUNDWXFtLysyNVgyUE5QcmQxS21wZ3RtcWJIVFNRblZPUCtDNXVaMlZiTVZzZVg0ZjQxTlRHOHdBMg0KYllWcDNFMzJ2dlZ"
                + "1cjlpMURjcnB4VjFCc2tSVmY4WjhrdEt3V2hVbUoxZnp4NTdLdG9RRU1VNmZ0SW1iS3dUMw0KcWt3a3BHZC85KzRGTVFPcE81bGFpTX"
                + "ZDTys2UU96VkIxZUN0T2lXTFcvK1lPdTBCR0FjYllwN25za1JPNytJTg0KdzRwUkoxZWhqcmNhRGZZdTVEL0hYZnpZREZFMk1BeExST"
                + "CtKaEVRQjVFelBocHdEVlVPZlRYeHF6bmdUaVhpNA0KT3hHdU9EL3cvdDdQYlJUOFdRZjYrYTdkYStYc3ZGai9nL2RHUWNsMjhhVlVG"
                + "ZEE2OGhiYktRc0MzemhTdmVvdw0Ka1czaVY5RTB5dk5PQ1BiMkluUXJ5TzdwLzBzT1BtL21CMmNJellaS04rdWNmWjhLZzh1U21vbGh"
                + "mNGdtZkZROA0KZ0pab0VOUnA0NSt4MnRXT3UxWXJiTWtyOUFDUUZ4TGVUMmpYanR0ZlNUckJWQXFqZmJsS2hOUE9uQ2RsMlpBTA0KVG"
                + "dNbit5cDZkWDhJS1FaVk8zaklaTm9VWUFrVGdsd0VoMllDL1h4bi9ZVGpobUxxYkFSTmpQd28xeXhGbWcrdg0KNSs4LzJYeE11ZDdOd"
                + "VNGS1o4dVdQTmpoZk9wUGtEOUlsWDlkb3I1QlFvYVYzWXM2emVyK09QTktFUHNYOFFmZA0KMnJXQXhzS3BMMFFQc0ZqUEo5ZWVDMFE0"
                + "SSs3LzRaa0ZSQlNqbVdXcmpFanlJb2x2dFJGOC9IUXV4U1ZHVlZsag0KYWtqMWU2SUdaYmw4MWtCKytGYkxPOWV1KzJKbEM2UnRIbUN"
                + "HMUIyWUIzT0UwS0JDWGJKVlBndUtCcWN1N1FFUA0KZDJJd2NsVWhUR0xGV0duRlp0TTRBWU1nUjlCeVl2WENYMklwMWNhTXdYYWg1c2"
                + "ZNWVZFaW9rRXlMZFprS1orRA0KeTJGZzJnYjZKUHUzc0FvQXB6WXBQc0F4Z1Yxa1JVU2hLRHl1TURydHUxaE1pSGpkUGNEUXpVaVA4T"
                + "WsvekUyWA0KbmIxTUloZWtkOUVFZ2dKWURvS0F2SnpkOTExME5PQlh5eE9wK2lZckZBbDQ3aENYdjlhRWt3MzMwTTE2Z3p6VQ0KekdM"
                + "NlZPUHVyeTZZa1J6SFVUbmQ3cXdkd092RFFmR1NwK2FQcjBweDlvYTNtbGYwbU1nL0pQSTJjdUV4NEUxeA0KTmc2aWhFai9rZUd3Mlp"
                + "DK2FQUTh4S08vN1pMU2xhRDRIK0IvZ0pjVjJiOHJhVEFhVFZNN0JOVStaejdqcjBOUg0KcUo0REVTWjk1NE9YNnZFcE96bXVoam1zNm"
                + "VFdDFZU0xvT1IwWFJydkVxTXJFaitTQjUzakNrOERKWUY0U1lzTw0KZHh5bWlkM1Y2Z1NONDVyanc3VkZBbE5UUDQ2UmFGVHdncmVPb"
                + "WpzeHpnNHlKK0dhamF2SG9qMHMyYTZYUjBZSA0KWnNIcUJ4YW9ZckRoaFQzNWJzWVgyQVMzNjlodUh1SCthdHZTNWZBREk5OWkxUkx1"
                + "b2ErS0k2c1lPamQ3U25Vcw0Kc2E3ajVpSWs2SHpxRTQxVGJubFN0TDVRWUxNeFFwMGMrVWdSSHcxYkNjcVpBS2svOG9wS2NKRkQ5YzB"
                + "STjdhSg0KYTdaZ0lNeEI3c1Z5UWpGWHZqNDhjVmhsalRsQ1ZsRURScHQrRSs5dzJ1czVTZi9Jb0xLaG9GVGJldW9UZzhCYw0KQU40NE"
                + "JjbmZPZnZqdXRmdUR4WlRaWXI3eVRpb0NMU2pBeGRLckpnM1pXMjZvcVNOSjRzckZ0aWVSY1FQWWkxZQ0KbFBaL1BaVmNVdVRacXhzY"
                + "k9uK1kzSEgvQVgwNXM4QmJRUW5hakNCN1lSOGNxUi8vbGVVMEtwUWYxQ1NxTTc3cA0KNEs2TEFkVlJMVit3U1h1WTNRUFU4YklPeTYr"
                + "YTdPRzloam51dE1Ba1J1VC9IM2lkTVdDMVU3RlZqSU9DM2lmeA0KdHNXOGJwL09ZQ0o3RFloQ2Q1L2F0elFVeU5mdy8vR2Ywc01aMkt"
                + "yZWtDTkhJVHJmV3ZCN3FtNTd6L01CcEloVA0KNDdHNVBPaDJrR1JQeE9YM0xXRGhhL0ZUdnF0blprWmcyNCt5clhSTXFMRVRxdDRnQU"
                + "FBQUFBQUFBQUFBQUE9PQ0KLS0tLS1FTkQgQ01TLS0tLS0NCg==");

        System.out.println("Writing data with length " + byteContent.length);

        for (int i=0 ; i<10 ; i++) {

            amazonS3.putObject(
                new PutObjectRequest(
                    BUCKET,
                    KEY,
                    new ByteArrayInputStream(byteContent),
                    new ObjectMetadata()
                )
            );

            amazonS3Https.putObject(
                new PutObjectRequest(
                    BUCKET,
                    KEY,
                    new ByteArrayInputStream(byteContent),
                    new ObjectMetadata()
                )
            );

        }
    }

    private static void setUpS3Server() throws IOException {
        s3Server = new S3Server(new InetSocketAddress(S3_SERVER_PORT), HTTPS.DISABLED);
        s3Server.start();

        amazonS3 = getAmazonS3Client(s3Server);
        amazonS3.createBucket(BUCKET);
    }

    private static void setUpS3ServerHttps() throws IOException {
        s3ServerHttps = new S3Server(new InetSocketAddress(S3_SERVER_PORT_HTTPS), HTTPS.ENABLED);
        s3ServerHttps.start();

        amazonS3Https = getAmazonS3Client(s3ServerHttps);
        amazonS3Https.createBucket(BUCKET);
    }

    public static AmazonS3Client getAmazonS3Client(S3Server s3Server) {
        AmazonS3Client client = new AmazonS3Client();
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        client.setEndpoint(s3Server.getAddress());
        return client;
    }
}
