package com.carledwinti.deserializer;

import com.carledwinti.model.Venda;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class VendaDeserializer implements Deserializer<Venda> {
    @Override
    public Venda deserialize(String topic, byte[] data) {

        try {
            Venda venda = new ObjectMapper().readValue(data, Venda.class);
            return venda;
        } catch (JsonParseException e) {
            System.err.println(e.getMessage());
        } catch (JsonMappingException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
        return null;
    }
}
