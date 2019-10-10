import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by LUJH13 on 2019-3-1.
 */
public class Test {



    static {

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

    }

    public static void main(String[] args) {

        InputStream in = null;

        try {

            in = new URL("hdfs://10.18.37.66:8020/udf/ssl-client.xml").openStream();

            IOUtils.copyBytes(in, System.out, 4096, false);

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            IOUtils.closeStream(in);

        }



    }
}
