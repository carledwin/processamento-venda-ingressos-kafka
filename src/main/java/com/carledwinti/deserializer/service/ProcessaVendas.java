package com.carledwinti.deserializer.service;

import com.carledwinti.deserializer.VendaDeserializer;
import com.carledwinti.model.Venda;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ProcessaVendas {

    public static void processa(){

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VendaDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "process-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //limite de mensagens que podem ser pegas por vez - default 500 para diminuir o lag(atraso) de leitura das mensagens
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        List<String> topics = Arrays.asList("venda-ingressos");
        try(KafkaConsumer<String, Venda> vendaKafkaConsumer = new KafkaConsumer<>(properties);){
            vendaKafkaConsumer.subscribe(topics);

            while(true){
                ConsumerRecords<String, Venda> vendaConsumerRecords = vendaKafkaConsumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, Venda> consumerRecord: vendaConsumerRecords){
                    Venda venda = consumerRecord.value();
                    executaVenda(venda);
                    Thread.sleep(500);
                    System.out.println(venda);
                }
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private static void executaVenda(Venda venda){

        if(new Random().nextBoolean()){
            venda.setStatus("APROVADA");
        }else{
            venda.setStatus("REjEITADA");
        }
    }
}
