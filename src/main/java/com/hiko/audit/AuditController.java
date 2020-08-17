package com.hiko.audit;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;

@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private AuditService service;

    public AuditController(AuditService service) {
        this.service = service;
    }

    @GetMapping
    public String auditLogByTime(long from, long to) throws IOException {
        File inputDir = new File("C:\\Project\\balance-transaction\\target\\logs\\input");
        File processDir = new File("C:\\Project\\balance-transaction\\target\\logs\\redis\\1");
        File outputDir = new File("output");
        service.auditThreeLogByTime(inputDir, processDir, outputDir, from, to);
        return "OK";
    }

    @GetMapping("/two-log-by-time")
    public String auditTwoLogByTime(long from, long to) throws IOException {
        File inputDir = new File("C:\\Project\\balance-transaction\\target\\logs\\input\\2020-08-Copy");
        File processDir = new File("C:\\Project\\balance-transaction\\target\\logs\\redis\\1\\2020-08-Copy");
        service.auditTwoLogByTime(inputDir, processDir, from, to);
        return "OK";
    }
}
