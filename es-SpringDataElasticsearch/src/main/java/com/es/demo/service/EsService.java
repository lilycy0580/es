package com.es.demo.service;

import com.es.demo.entity.Hotel;
import com.es.demo.repository.EsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EsService {

    @Autowired
    EsRepository esRepository;

    public List<Hotel> getHotelFromTitle(String keyword){
        return  esRepository.findByTitleLike(keyword);
    }
}
