package com.example.stoelasticsearchdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("elasticSearch")
public class esController {

    @Autowired
    EsService esService;

    @GetMapping(value = "/create")
    public void create(String index) {
        esService.createIndex(index);
    }

    @GetMapping(value = "/delete")
    public void delete(String index) {
        esService.deleteIndex(index);
    }

    @GetMapping(value = "/isIndexExists")
    public boolean isIndexExists(String index) {
        return esService.isIndexExists(index);
    }

    @GetMapping(value = "/insertIndex")
    public String insertIndex(String index, String ids, String content) {
        Map<String, String> data = new HashMap<>();
        data.put("id", ids);
        data.put("content", content);
        return esService.insertIndex(index, ids, data);
    }

    @GetMapping(value = "/getIndex")
    public String getIndex(String index, String ids) {
        return esService.getById(index, ids);
    }

    @GetMapping(value = "/putIndex")
    public String putIndex(String index, String ids, String content) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", content.length());
        data.put("content", content);
        return esService.putIndex(index, ids, data);
    }

    @GetMapping(value = "/delIndex")
    public String delIndex(String index, String ids) {
        return esService.delIndex(index, ids);
    }
}
