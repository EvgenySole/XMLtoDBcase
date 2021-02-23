package com.XMLtoDB;

import java.io.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;


import org.w3c.dom.*;
import javax.xml.parsers.*;

public class ParseXML {
    public static void main(String[] args) {
    try {
        Class.forName("org.postgresql.Driver");

        System.out.println("Введите адрес базы данных");
        //Connection ddd = DriverManager.getConnection("jdbc:postgresql://localhost:5432/plant");C:\Krista\data
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String url = reader.readLine();
        System.out.println("Введите пользователя");
        String user = reader.readLine();
        System.out.println("Введите пароль");
        String password = reader.readLine();
        Connection con = DriverManager.getConnection(url,user,password);
        Statement st=con.createStatement();
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        System.out.println("Введите путь к файлу или папку с файлами");
        String path = reader.readLine();
        List<Path> collect = Files.walk(Paths.get(path)).filter(Files::isRegularFile).collect(Collectors.toList());
        System.out.println("Выбраны файлы - " + collect);
        for (int i = 0; i < collect.size(); i++) {
            Document doc = docBuilder.parse(new File(String.valueOf(collect.get(i))));
            System.out.println("Обрабатывается  " + collect.get(i) + "...");
            doc.getDocumentElement().normalize();

            System.out.println("Загрузка атрбутов - " + doc.getDocumentElement().getNodeName());
            Node node = doc.getChildNodes().item(0);
            NamedNodeMap companyList = node.getAttributes();
            Node nameAttrib = companyList.getNamedItem("company");
            String company = nameAttrib.getNodeValue();
            NamedNodeMap dateList = node.getAttributes();
            Node dateAttrib = dateList.getNamedItem("date");
            String date1 = dateAttrib.getNodeValue();
            String day = date1.substring(0, 2);
            String month = date1.substring(3, 5);
            String year = date1.substring(6, 10);
            String date = (year + "." + month + "." + day);

            NamedNodeMap uuidList = node.getAttributes();
            Node uuidAttrib = uuidList.getNamedItem("uuid");
            String uuid = uuidAttrib.getNodeValue();

            int tt = st.executeUpdate("insert into d_cat_catalog(delivery_date,company,uuid) values('" + date + "' ,'" + company + "' , '" + uuid + "');");
            System.out.println("Данные успешно загружены: " + "Company - " + company + " " + " Date - " + date + " UUID - " + uuid);
            String catalogid = "";
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT id FROM d_cat_catalog");
            while (rs.next()) {
                catalogid = rs.getString("id");
            }

            System.out.println("Загрузка элементов - Plant");
            NodeList listOfPlants = doc.getElementsByTagName("PLANT");
            for (int s = 0; s < listOfPlants.getLength(); s++) {
                Node firstPlantNode = listOfPlants.item(s);
                if (firstPlantNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element firstPersonElement = (Element) firstPlantNode;
                    NodeList commonList = firstPersonElement.getElementsByTagName("COMMON");
                    Element commonElement = (Element) commonList.item(0);

                    NodeList commonTextList = commonElement.getChildNodes();
                    String common = ((Node) commonTextList.item(0)).getNodeValue().trim();

                    NodeList botanicalList = firstPersonElement.getElementsByTagName("BOTANICAL");
                    Element botanicalElement = (Element) botanicalList.item(0);

                    NodeList botanicalTextList = botanicalElement.getChildNodes();
                    String botanical = ((Node) botanicalTextList.item(0)).getNodeValue().trim();

                    NodeList zoneList = firstPersonElement.getElementsByTagName("ZONE");
                    Element zoneElement = (Element) zoneList.item(0);

                    NodeList zoneTextList = zoneElement.getChildNodes();
                    String zone = ((Node) zoneTextList.item(0)).getNodeValue().trim();
                    if (zone.length() > 5) zone = "1";
                    else zone = zone.substring(0, 1);

                    NodeList lightList = firstPersonElement.getElementsByTagName("LIGHT");
                    Element lightElement = (Element) lightList.item(0);

                    NodeList lightTextList = lightElement.getChildNodes();
                    String light = ((Node) lightTextList.item(0)).getNodeValue().trim();

                    NodeList priceList = firstPersonElement.getElementsByTagName("PRICE");
                    Element priceElement = (Element) priceList.item(0);

                    NodeList priceTextList = priceElement.getChildNodes();
                    String price1 = ((Node) priceTextList.item(0)).getNodeValue().trim();
                    String price = price1.substring(1);

                    NodeList availabilityList = firstPersonElement.getElementsByTagName("AVAILABILITY");
                    Element availabilityElement = (Element) availabilityList.item(0);

                    NodeList availabilityTextList = availabilityElement.getChildNodes();
                    String availability = ((Node) availabilityTextList.item(0)).getNodeValue().trim();

                    int ss = st.executeUpdate("insert into f_cat_plants(common,botanical,zone,light,price,availability,catalog_id) values('" + common + "' ,'" + botanical + "' , '" + zone + "' , '" + light + "' , '" + price + "' , '" + availability + "' , '" + catalogid + "');");
                }
            }
            System.out.println("Данные успешно загружены в базу данных");

        }
    }catch (Exception err) {
        System.out.println("Error " + err.getMessage ());
    }

}


}
