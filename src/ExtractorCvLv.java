package com.simargl.textutil.extractor.site;

import com.simargl.textutil.extractor.ExtractorFromSite;
import com.simargl.textutil.parser.ParserData;
import com.simargl.textutil.parser.ParserTypeDataEnum;
import com.simargl.textutil.parser.extractor.BirthDateExtractor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by MyMac on 6/24/16.
 */
public class ExtractorCvLv implements ExtractorFromSite {
    @Override
    public List<String> getText(Document doc, URL url) {
        return doc.select(".popup-content").select("*").stream()
                .flatMap(el -> el.textNodes().stream()).map(node -> node.text())
                .filter(content -> !content.trim().isEmpty()).collect(Collectors.toList());
    }

    @Override
    public String getHTML(Document doc1, URL url) {

        Document doc = doc1.clone();
        Elements ele = doc.getAllElements();
        for (int j = 0; j < ele.size(); j++) {
            Element element = ele.get(j);
            if (element.className().equals("cvVormHeading")) {
                String text = element.text();
                if (text.equals("Опыт работы") || text.equals("Work experience") || text.equals("Darba pieredze")) {
                    Elements tr = ele.get(j + 2).child(0).children();
                    for (int i = 0; i < tr.size(); i++) {
                        Elements tds = tr.get(i).children();
                        for (int k = 0; k < tds.size(); k++) {
                            Element td = tds.get(k);
                            td.append("<br>");
                        }
                    }
                }
            }
        }
        Elements tables = doc.select(".popup-content > table");
        for (int j = 0; j < tables.size(); j++) {
            Boolean removeTable = false;
            Element table = tables.get(j);
            Elements rows = table.select("tr");
            for (int i = 0; i < rows.size(); i++) {
                Element row = rows.get(i);
                Elements cols = row.select("td");
                String firstCol = cols.get(0).text();
                if (firstCol.equals("Add") || firstCol.equals("Добавить") || firstCol.equals("Pievienot")) {
                    row.remove();
                }
//                if (firstCol.equals("Желаемые должности:") || firstCol.equals("Preferred occupations:") || firstCol.equals("Vēlamie amati")) {
//                    row.remove();
//                }
                if (firstCol.equals("Пол:") || firstCol.equals("Sex:") || firstCol.equals("Dzimums:")) {
                    row.remove();
                }
                if (firstCol.equals("CV accounts") || firstCol.equals("CV папки") || firstCol.equals("CV katalogi")) {
                    row.remove();
                }
                if (firstCol.equals("Количество просмотров") || firstCol.equals("Total number of previews") || firstCol.equals("Reizes skatīts")) {
                    table.remove();
                    removeTable = true;
                }
                if (firstCol.equals("Name:") || firstCol.equals("Имя:") || firstCol.equals("Vārds:")) {
                    row.remove();
                }
                if (firstCol.equals("Дата рождения:") || firstCol.equals("Birth date:") || firstCol.equals("Dzimšanas datums:")) {
                    row.remove();
                }
                if (firstCol.equals("Адрес:") || firstCol.equals("Address:") || firstCol.equals("Adrese:")) {
                    row.remove();
                }
                if (firstCol.equals("Электронная почта:") || firstCol.equals("E-mail:") || firstCol.equals("E-pasts:")) {
                    row.remove();
                }
                if (firstCol.equals("Skype:")) {
                    row.remove();
                }
                if (firstCol.equals("Контактный телефон:") || firstCol.equals("Contact telephone:") || firstCol.equals("Kontakttālrunis:")) {
                    row.remove();
                }
            }
            if(!removeTable){
                table.after("<br><br>");
            }
        }
        Elements elements = doc.getAllElements();
        for (int j = 0; j < elements.size(); j++) {
            Element element = elements.get(j);
            if (element.className().equals("cvVormHeading")) {
                element.append("<br><br>");
                String text = element.text();
                if (text.equals("CV и папки") || text.equals("CV and foders") || text.equals("Katalogi, kuros saglabāti CV")) {
                    element.remove();
                }
                if (text.equals("Просмотр информации в CV") || text.equals("CV viewing information") || text.equals("CV apskates informācija")) {
                    element.remove();
                }
                if (text.equals("Дополнительные CV пользователя") || text.equals("User additional CV-s")) {
                    element.remove();
                    elements.get(j + 2).remove();
                }
                if (text.equals("Комментарии к CV") || text.equals("CV comments") || text.equals("CV komentāri")) {
                    element.remove();
                }
                if (text.equals("Personal Data") || text.equals("Личные данные") || text.equals("Personas dati")) {
                    element.remove();
                }
            }
        }
        Elements elementsByAttributeValue = doc.getElementsByAttributeValue("class", "textSmall");
        if(CollectionUtils.isNotEmpty(elementsByAttributeValue)){
            elementsByAttributeValue.first().remove();
        }

        doc.select(".popup-big").remove();
        doc.select(".top_inf").remove();
        doc.select(".textred").remove();
        doc.select(".margin-15").remove();
        doc.select(".cleverStaffBlock").remove();


        return doc.select(".popup-content").html();
    }

    @Override
    public String getImgSrc(Document doc, URL url) {
        Elements tables = doc.select(".popup-content > table");
        if (tables != null) {
            Element table = tables.first();
            Elements rows = table.select("tr");
            if (rows != null) {
                Element row = rows.first();
                Elements cols = row.select("td");
                System.out.println("!!"+cols.size());
                if (cols != null) {
                    String firstCol = cols.first().text();
                    if (firstCol.equals("Имя:") || firstCol.equals("Name:")|| firstCol.equals("Vārds:")) {
                        Element secondCol = cols.get(1);
                        if (CollectionUtils.isNotEmpty(secondCol.children())) {
                            Element select = secondCol.child(0).child(0).child(0);
                            if (select != null) {
                                String attr = select.attr("src").substring(2);
                                if (StringUtils.isNotBlank(attr)) {
                                    String img = "https://" + attr;
                                    return img;
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<String> getTextFromPlugin(Document doc, URL url) {
        return null;
    }

    @Override
    public String getHTMLFromPlugin(Document doc, URL url) {
        return getHTML(doc, url);
    }

    @Override
    public String getAttachmentUrl(Document doc, URL url) {
        String href = null;
        Elements select = doc.getElementsByAttributeValue("class", "cvpdflink");
        if (CollectionUtils.isNotEmpty(select)) {
            href = select.get(0).attr("href");
            if (StringUtils.isNotBlank(href)) {
                return "http://www.cv.lv" + href;
            }
        }
        return href;
    }

    @Override
    public void putSpecialInfo(Document doc, URL url, ParserData pd) {
        Boolean firstOccupations = false;
        Elements tables = doc.select(".popup-content > table");
        String preferredOccupations = null;
        String firstPreference = null;
        for (int j = 0; j < tables.size(); j++) {
            Element table = tables.get(j);
            Elements rows = table.select("tr");
            for (int i = 0; i < rows.size(); i++) {
                Element row = rows.get(i);
                Elements cols = row.select("td");
                String firstCol = cols.get(0).text();
                if (firstCol.equals("Желаемые должности:") || firstCol.equals("Preferred occupations:")|| firstCol.equals("Vēlamie amati (1):")) {
                    if(!firstOccupations){
                        preferredOccupations = cols.get(1).text().split(",")[0];
                        firstOccupations = true;
                    }
                }
                if (firstCol.equals("First preference") || firstCol.equals("Первое предпочтение")|| firstCol.equals("Pirmā izvēle")) {
                    firstPreference = cols.get(1).text().split(",")[0];
                }
                if (firstCol.equals("Пол:") || firstCol.equals("Sex:") || firstCol.equals("Dzimums:")) {
                    String gender = cols.get(1).text();
                    if (gender.contains("Male") || gender.contains("Мужчина") || gender.contains("Vīrietis")) {
                        pd.putDataByKeyClear(ParserTypeDataEnum.gender, "true");
                    } else if (gender.contains("Женщина") || gender.contains("Female") || gender.contains("Sieviete")) {
                        pd.putDataByKeyClear(ParserTypeDataEnum.gender, "false");
                    }
                }
                if (firstCol.equals("Дата рождения:") || firstCol.equals("Birth date:") || firstCol.equals("Dzimšanas datums:")) {
                    String db = cols.get(1).text();
                    BirthDateExtractor birthDatgleExtractor = new BirthDateExtractor();
                    birthDatgleExtractor.parse(Arrays.asList(db), pd);
                }

                if (firstCol.equals("Имя:") || firstCol.equals("Name:") || firstCol.equals("Vārds:")) {
                    String fullName = cols.get(1).text();
                    pd.putDataByKeyClear(ParserTypeDataEnum.fullname, fullName);
                }
                if (firstCol.equals("Электронная почта:") || firstCol.equals("E-mail:") || firstCol.equals("E-pasts:")) {
                    String email = cols.get(1).text();
                    pd.putDataByKey(ParserTypeDataEnum.email, email);
                }
                if (firstCol.equals("Контактный телефон:") || firstCol.equals("Contact telephone:") || firstCol.equals("Kontakttālrunis:")) {
                    String phone = cols.get(1).text();
                    pd.putDataByKey(ParserTypeDataEnum.phone, phone);
                }
                if (firstCol.equals("Skype:")) {
                    String skype = cols.get(1).text();
                    pd.putDataByKey(ParserTypeDataEnum.skype, skype);
                }
                if (firstCol.equals("Address:") || firstCol.equals("Адрес:") || firstCol.equals("Adrese:")) {
                    String[] adress = cols.get(1).text().split(",");
                    if(adress.length == 1 ){
                        pd.putDataByKey(ParserTypeDataEnum.country, adress[0]);
                    }
                    if(adress.length == 2 ){
                        pd.putDataByKey(ParserTypeDataEnum.country, adress[adress.length -1]);
                    }
                    if(adress.length > 2 || adress.length == 2){
                        pd.putDataByKey(ParserTypeDataEnum.country, adress[adress.length -1]);
                        pd.putDataByKey(ParserTypeDataEnum.city, adress[0]);
                    }
                }
            }
        }
        if(preferredOccupations != null){
            pd.putDataByKeyClear(ParserTypeDataEnum.position, preferredOccupations);
        }else {
            if(firstPreference != null){
                pd.putDataByKeyClear(ParserTypeDataEnum.position, firstPreference);
            }
        }
    }
}
