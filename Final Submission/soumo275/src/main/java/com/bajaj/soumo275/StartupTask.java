package com.bajaj.soumo275;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class StartupTask implements CommandLineRunner {

    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void run(String... args) throws Exception {

        String url = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

        String body = """
                {
                   "name": "Soumyadip Mukherjee",
                   "regNo": "22BRS1127",
                   "email": "soumyadip.mukherjee2022@vitstudent.ac.in"
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = rest.postForEntity(url, request, String.class);

        System.out.println("\n===== RAW RESPONSE FROM generateWebhook =====");
        System.out.println(response.getBody());

        // Parse JSON safely
        JsonNode json = mapper.readTree(response.getBody());

        JsonNode webhookNode = json.get("webhook");
        JsonNode tokenNode = json.get("accessToken");

        if (webhookNode == null || tokenNode == null) {
            System.out.println("ERROR: API did NOT return expected fields!");
            System.out.println("Full JSON:\n" + json.toPrettyString());
            return;
        }

        String webhookUrl = webhookNode.asText();
        String accessToken = tokenNode.asText();

        System.out.println("Webhook URL = " + webhookUrl);
        System.out.println("JWT Token   = " + accessToken);

        String finalSQLQuery = """
                WITH emp_totals AS (
                    SELECT
                        d.DEPARTMENT_NAME,
                        e.EMP_ID,
                        e.FIRST_NAME || ' ' || e.LAST_NAME AS EMPLOYEE_NAME,
                        e.DOB,
                        SUM(CASE WHEN EXTRACT(DAY FROM p.PAYMENT_TIME) != 1 THEN p.AMOUNT END) AS SALARY
                    FROM EMPLOYEE e
                    JOIN DEPARTMENT d
                        ON e.DEPARTMENT = d.DEPARTMENT_ID
                    LEFT JOIN PAYMENTS p
                        ON e.EMP_ID = p.EMP_ID
                    GROUP BY d.DEPARTMENT_NAME, e.EMP_ID, e.FIRST_NAME, e.LAST_NAME, e.DOB
                )
                SELECT
                    DEPARTMENT_NAME,
                    SALARY,
                    EMPLOYEE_NAME,
                    FLOOR(EXTRACT(YEAR FROM AGE(DOB))) AS AGE
                FROM (
                    SELECT
                        emp_totals.*,
                        ROW_NUMBER() OVER (PARTITION BY DEPARTMENT_NAME ORDER BY SALARY DESC NULLS LAST) AS rn
                    FROM emp_totals
                ) ranked
                WHERE rn = 1;
                """;

        String escapedQuery = mapper.writeValueAsString(finalSQLQuery);

        // String submitUrl =
        // "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";

        String submitUrl = webhookUrl;

        HttpHeaders submitHeaders = new HttpHeaders();
        submitHeaders.setContentType(MediaType.APPLICATION_JSON);
        submitHeaders.set("Authorization", accessToken); // JWT token

        String submitBody = """
                {
                   "finalQuery": %s
                }
                """.formatted(escapedQuery);

        HttpEntity<String> submitReq = new HttpEntity<>(submitBody, submitHeaders);

        System.out.println("\n===== SENDING FINAL SQL TO WEBHOOK =====");
        System.out.println(submitBody);

        ResponseEntity<String> submitResponse = rest.postForEntity(submitUrl, submitReq, String.class);

        System.out.println("\n===== WEBHOOK SUBMIT RESPONSE =====");
        System.out.println(submitResponse.getBody());
    }
}
