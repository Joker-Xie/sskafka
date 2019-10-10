import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Calendar;

class DemoProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        Calendar now = ;
        System.out.println("day: "+ Calendar.getInstance());
    }
}

