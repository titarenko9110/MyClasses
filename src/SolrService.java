package com.simargl.recrut.service;

import com.simargl.recrut.domain.Skill;
import com.simargl.recrut.dto.RecrutDocument;
import com.simargl.recrut.dto.RecrutDocumentSearch;
import com.simargl.recrut.enums.ObjectEnum;
import com.simargl.recrut.enums.SearchFullTextTypeEnum;
import com.simargl.recrut.repo.solr.RecrutDocumentRepository;
import com.simargl.serverlib.logging.LogFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.*;
import org.springframework.data.solr.core.query.result.HighlightPage;
import org.springframework.data.solr.core.query.result.ScoredPage;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author curator
 */
@Service
public class SolrService {

    @Qualifier("logFactory")
    @Autowired
    protected LogFactory logFactory;
    protected Logger logger;
    protected Logger loggerError;

    @Resource
    private RecrutDocumentRepository solrRepo;

    @Resource
    private SolrTemplate template;

    private final BlockingQueue<RecrutDocument> documentIndexQueue = new LinkedBlockingQueue<>(20000);

    private static final String EMAIL_PATTERN
            = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    @PostConstruct
    private void postConstruct() {
        logger = LogFactory.getLogger(this.getClass().getSimpleName());
        loggerError = LogFactory.getLogger(this.getClass().getSimpleName() + "_ERROR");
    }

    public HighlightPage<RecrutDocumentSearch> search(final String orgId, final ObjectEnum category,
                                                      PageRequest pageRequest, final String searchStr,
                                                      SearchFullTextTypeEnum searchFullTextTypeEnum, final boolean withContent) {
        if (searchFullTextTypeEnum == null || searchFullTextTypeEnum == SearchFullTextTypeEnum.ordinary) {
            searchFullTextTypeEnum = SearchFullTextTypeEnum.and;
        }
        String searchString = searchStr.replaceAll("\\s+", " ").replaceAll("\\s*\\/\\s*", " ").toLowerCase();

        String[] words = StringUtils.split(searchString, " .");
        if (words.length == 0) {
            searchFullTextTypeEnum = SearchFullTextTypeEnum.full_match;
        }
        if (category.equals(ObjectEnum.candidate)
                && searchString.replaceAll("[^0-9]", "").length() > 3
                && searchString.replaceAll("[^A-Za-zА-Яа-я]", "").isEmpty()) {
            words = new String[]{searchString.replaceAll("[^0-9]", "")};
        }
        if (category.equals(ObjectEnum.candidate)) {
            String[] words1 = searchString.split("[\\s]+");
            Pattern pattern = Pattern.compile(EMAIL_PATTERN);
            if (words1.length == 1 && pattern.matcher(words1[0]).matches()) {
                searchFullTextTypeEnum = SearchFullTextTypeEnum.full_match;
                searchString = words1[0].replaceAll("\\.", " ");
            }
        }
        List<Criteria> criterias = new ArrayList<>();
        criterias.add(new Criteria("orgId").is(orgId));
        criterias.add(new Criteria("category").is(category.toString()));
        HighlightOptions highlightOptions = new HighlightOptions();
        highlightOptions.setSimplePostfix("</highlight>");
        highlightOptions.setSimplePrefix("<highlight>");
        highlightOptions.addField("content");
        Criteria text = null;
        Criteria head = null;
        Criteria titleFull = null;
        Criteria skills = null;
        Criteria descr = null;
        Criteria conditions = null;
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.full_match) {
            text = new Criteria("text").is(searchString).boost((float) 2);
            if (Arrays.asList(words).size() == 1) {
                titleFull = new Criteria("position").is(searchString).boost((float) 128);
            }
            Criteria b = new Criteria("head").is(searchString).boost((float) 16);
            Criteria c = new Criteria("skills").is(searchString).boost((float) 8);
            Criteria d = new Criteria("descr").is(searchString).boost((float) 4);
            if (titleFull == null) {
                conditions = text.or(b).or(c).or(d);
            } else {
                conditions = text.or(titleFull).or(b).or(c).or(d);
            }
        }
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.and) {
            List<String> wordsList = Arrays.asList(words);
            Integer boost = 1024;
            List<Criteria> newCreterias = new ArrayList<>();
            Criteria forOneWord  = null;
            for (String word : wordsList) {
                if (wordsList.size() > 1) {
                    if (text == null) {
                        text = new Criteria("text").is(word).boost((float) 2);
                        boost = boost - 64;
                        newCreterias.add(new Criteria("head").contains(word).boost((float) boost));
                        head = new Criteria("head").is(word).boost((float) 2048);
                        skills = new Criteria("skills").is(word).boost((float) 8);
                        descr = new Criteria("descr").is(word).boost((float) 4);
                    } else {
                        text = text.and(new Criteria("text").is(word).boost((float) 2));
                        if(boost <= 512){
                            boost = 512;
                        }else {
                            boost = boost - 64;
                        }
                        newCreterias.add(new Criteria("head").contains(word).boost((float) boost));
                        head = head.and(new Criteria("head").is(word).boost((float) 2048));
                        skills = skills.and(new Criteria("skills").is(word).boost((float) 8));
                        descr = descr.and(new Criteria("descr").is(word).boost((float) 4));
                    }
                } else {
                    if (word.equalsIgnoreCase("java") || word.equalsIgnoreCase("c#")) {
                        text = new Criteria("text").is(word).boost((float) 2);
                        titleFull = new Criteria("position").is(word).boost((float) 512);
                        head = new Criteria("head").is(word).boost((float) 16);
                        skills = new Criteria("skills").is(word).boost((float) 8);
                        descr = new Criteria("descr").is(word).boost((float) 4);
                    } else {
                        text = new Criteria("text").contains(word).boost((float) 2);
                        titleFull = new Criteria("position").is(word).boost((float) 2000);
                        forOneWord =  new Criteria("position").contains(word).boost((float) 1000);
                        head = new Criteria("head").contains(word).boost((float) 16);
                        skills = new Criteria("skills").contains(word).boost((float) 8);
                        descr = new Criteria("descr").contains(word).boost((float) 4);
                    }
                }
            }
            if (titleFull == null) {
                Criteria fullCriteria = null;
                for (Criteria r : newCreterias) {
                    if(fullCriteria  == null){
                        fullCriteria = r;
                    }else {
                        fullCriteria = fullCriteria.or(r);
                    }
                }
                conditions = fullCriteria.or(text).or(head).or(skills).or(descr);
            } else {
                if(forOneWord != null){
                    conditions = forOneWord.or(titleFull).or(text).or(head).or(skills).or(descr);
                }else {
                    conditions = titleFull.or(text).or(head).or(skills).or(descr);
                }
            }

        }
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.or) {
            if (words.length > 1) {
                text = new Criteria("text").is(Arrays.asList(words)).boost((float) 2);
                head = new Criteria("head").is(Arrays.asList(words)).boost((float) 16);
                skills = new Criteria("skills").is(Arrays.asList(words)).boost((float) 8);
                descr = new Criteria("descr").is(Arrays.asList(words)).boost((float) 4);
            } else {
                if (words.length == 1
                        && (words[0].equalsIgnoreCase("java") || words[0].equalsIgnoreCase("c#"))) {
                    text = new Criteria("text").is(searchString).boost((float) 2);
                    titleFull = new Criteria("position").is(searchString).boost((float) 128);
                    head = new Criteria("head").is(searchString).boost((float) 16);
                    skills = new Criteria("skills").is(searchString).boost((float) 8);
                    descr = new Criteria("descr").is(searchString).boost((float) 4);
                } else {
                    text = new Criteria("text").contains(Arrays.asList(words)).boost((float) 2);
                    titleFull = new Criteria("position").is(Arrays.asList(words)).boost((float) 128);
                    head = new Criteria("head").contains(Arrays.asList(words)).boost((float) 16);
                    skills = new Criteria("skills").contains(Arrays.asList(words)).boost((float) 8);
                    descr = new Criteria("descr").contains(Arrays.asList(words)).boost((float) 4);
                }
            }
            if (titleFull == null) {
                conditions = text.or(head).or(skills).or(descr);
            } else {
                conditions = text.or(titleFull).or(head).or(skills).or(descr);
            }
        }
        Criteria main = null;
        for (Criteria criteria : criterias) {
            if (main == null) {
                main = criteria;
            } else {
                main = main.and(criteria);
            }
        }


        SimpleHighlightQuery search = new SimpleHighlightQuery(main.and(conditions));
        search.setPageRequest(pageRequest);
        if (withContent) {
            search.addProjectionOnFields("*", "score");
        } else {
            search.addProjectionOnFields("id", "orgId", "subject", "category", "score");
        }
        search.addSort(new Sort(Sort.Direction.DESC, "score"));
        HighlightPage<RecrutDocumentSearch> recrutDocumentSearches = template.queryForHighlightPage(search, RecrutDocumentSearch.class);
        return recrutDocumentSearches;
    }

    public HighlightPage<RecrutDocumentSearch> search(final String orgId, final ObjectEnum category, final String searchStr,
                                                      SearchFullTextTypeEnum searchFullTextTypeEnum) {
        return search(orgId, category, new PageRequest(0, 10000), searchStr, searchFullTextTypeEnum, false);
    }

    public HighlightPage<RecrutDocumentSearch> getHighlight(final Collection<String> objIds,
                                                            final String orgId, final ObjectEnum category,
                                                            final String searchStr, SearchFullTextTypeEnum searchFullTextTypeEnum) {
        if (searchFullTextTypeEnum == null || searchFullTextTypeEnum == SearchFullTextTypeEnum.ordinary) {
            searchFullTextTypeEnum = SearchFullTextTypeEnum.and;
        }
        String searchString = searchStr.replaceAll("\\s+", " ").replaceAll("\\s*\\/\\s*", " ").toLowerCase();
        String[] words = StringUtils.split(searchString, " .");
        if (category.equals(ObjectEnum.candidate)
                && searchString.replaceAll("[^0-9]", "").length() > 3
                && searchString.replaceAll("[^A-Za-zА-Яа-я]", "").isEmpty()) {
            words = new String[]{searchString.replaceAll("[^0-9]", "")};
        }
        if (words.length == 0) {
            searchFullTextTypeEnum = SearchFullTextTypeEnum.full_match;
        }
        if (category.equals(ObjectEnum.candidate)) {
            String[] words1 = searchString.split("[\\s]+");
            Pattern pattern = Pattern.compile(EMAIL_PATTERN);
            if (words1.length == 1 && pattern.matcher(words1[0]).matches()) {
                searchFullTextTypeEnum = SearchFullTextTypeEnum.full_match;
                searchString = words1[0].replaceAll("\\.", " ");
            }
        }
        List<Criteria> criterias = new ArrayList<>();
        criterias.add(new Criteria("orgId").is(orgId));
        criterias.add(new Criteria("category").is(category.toString()));
        criterias.add(new Criteria("subject").in(objIds));
        Criteria text = null;
        Criteria head = null;
        Criteria skills = null;
        Criteria descr = null;
        Criteria titleFull = null;
        HighlightOptions highlightOptions = new HighlightOptions();
        highlightOptions.setSimplePostfix("</highlight>");
        highlightOptions.setSimplePrefix("<highlight>");
        highlightOptions.addField("content");
        Criteria conditions = null;
        Crotch or = null;
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.full_match) {
            text = new Criteria("text").is(searchString).boost((float) 2);
            highlightOptions.setQuery(new SimpleHighlightQuery(text));
            if (Arrays.asList(words).size() == 1) {
                titleFull = new Criteria("position").is(searchString).boost((float) 128);
            }
            Criteria b = new Criteria("head").is(searchString).boost((float) 16);
            Criteria c = new Criteria("skills").is(searchString).boost((float) 8);
            Criteria d = new Criteria("descr").is(searchString).boost((float) 4);
            if (titleFull == null) {
                or = text.or(b).or(c).or(d);
            } else {
                or = text.or(titleFull).or(b).or(c).or(d);
            }
        }
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.and) {
            List<String> wordsList = Arrays.asList(words);
            Integer boost = 1024;
            Criteria forOneWord = null;
            List<Criteria> newCriterias = new ArrayList<>();
            for (String word : wordsList) {
                if (wordsList.size() > 1) {
                    if (text == null) {
                        text = new Criteria("text").is(word).boost((float) 2);
                        boost = boost - 64;
                        newCriterias.add(new Criteria("head").contains(word).boost((float) boost));
                        head = new Criteria("head").is(word).boost((float) 2048);
                        skills = new Criteria("skills").is(word).boost((float) 8);
                        descr = new Criteria("descr").is(word).boost((float) 4);
                    } else {
                        text = text.and(new Criteria("text").is(word).boost((float) 2));
                        if(boost <= 512){
                            boost = 512;
                        }else {
                            boost = boost - 64;
                        }
                        newCriterias.add(new Criteria("head").contains(word).boost((float) boost));
                        head = head.and(new Criteria("head").is(word).boost((float) 2048));
                        skills = skills.and(new Criteria("skills").is(word).boost((float) 8));
                        descr = descr.and(new Criteria("descr").is(word).boost((float) 4));
                    }
                } else {
                    if (word.equalsIgnoreCase("java") || word.equalsIgnoreCase("c#")) {
                        text = new Criteria("text").is(word).boost((float) 2);
                        titleFull = new Criteria("position").is(word).boost((float) 128);
                        head = new Criteria("head").is(word).boost((float) 16);
                        skills = new Criteria("skills").is(word).boost((float) 8);
                        descr = new Criteria("descr").is(word).boost((float) 4);
                    } else {
                        text = new Criteria("text").contains(word).boost((float) 2);
                        titleFull = new Criteria("position").is(word).boost((float) 2000);
                        forOneWord =  new Criteria("position").contains(word).boost((float) 1000);
                        head = new Criteria("head").contains(word).boost((float) 16);
                        skills = new Criteria("skills").contains(word).boost((float) 8);
                        descr = new Criteria("descr").contains(word).boost((float) 4);
                    }
                }
            }
            highlightOptions.setQuery(new SimpleHighlightQuery(text));
            if (titleFull == null) {
                Criteria fullCrieria = null;
                for (Criteria r : newCriterias) {
                    if(fullCrieria  == null){
                        fullCrieria = r;
                    }else {
                        fullCrieria = fullCrieria.or(r);
                    }
                }
                or = fullCrieria.or(text).or(head).or(skills).or(descr);
            } else {
                if(forOneWord != null){
                    or = forOneWord.or(titleFull).or(text).or(head).or(skills).or(descr);
                }else {
                    or = titleFull.or(text).or(head).or(skills).or(descr);
                }
            }
        }
        if (searchFullTextTypeEnum == SearchFullTextTypeEnum.or) {
            if (words.length > 1) {
                text = new Criteria("text").is(Arrays.asList(words)).boost((float) 2);
                head = new Criteria("head").is(Arrays.asList(words)).boost((float) 16);
                skills = new Criteria("skills").is(Arrays.asList(words)).boost((float) 8);
                descr = new Criteria("descr").is(Arrays.asList(words)).boost((float) 4);
            } else {
                if (words.length == 1
                        && (words[0].equalsIgnoreCase("java") || words[0].equalsIgnoreCase("c#"))) {
                    text = new Criteria("text").is(searchString).boost((float) 2);
                    titleFull = new Criteria("position").is(searchString).boost((float) 128);
                    head = new Criteria("head").is(searchString).boost((float) 16);
                    skills = new Criteria("skills").is(searchString).boost((float) 8);
                    descr = new Criteria("descr").is(searchString).boost((float) 4);
                } else {
                    text = new Criteria("text").contains(Arrays.asList(words)).boost((float) 2);
                    titleFull = new Criteria("position").contains(Arrays.asList(words)).boost((float) 128);
                    head = new Criteria("head").contains(Arrays.asList(words)).boost((float) 16);
                    skills = new Criteria("skills").contains(Arrays.asList(words)).boost((float) 8);
                    descr = new Criteria("descr").contains(Arrays.asList(words)).boost((float) 4);
                }
            }
            if (titleFull == null) {
                or = text.or(head).or(skills).or(descr);
            } else {
                or = text.or(titleFull).or(head).or(skills).or(descr);
            }
            highlightOptions.setQuery(new SimpleHighlightQuery(text));
        }
        for (Criteria criteria : criterias) {
            if (conditions == null) {
                conditions = criteria;
            } else {
                conditions = conditions.and(criteria);
            }
        }
        conditions.and(or);
        SimpleHighlightQuery search = new SimpleHighlightQuery(conditions);
        search.setPageRequest(new PageRequest(0, 10000));
        search.addProjectionOnFields("id", "orgId", "subject", "category", "score");
        search.addSort(new Sort(Sort.Direction.DESC, "score"));
        search.setHighlightOptions(highlightOptions);
        HighlightPage<RecrutDocumentSearch> recrutDocumentSearches = template.queryForHighlightPage(search, RecrutDocumentSearch.class);
        return recrutDocumentSearches;
    }

    public void remove(final String id) {
        try {
            solrRepo.delete(id);
        } catch (Exception ex) {
            LogFactory.logException(ex, logger);
        }
    }

    public RecrutDocument get(final String id) {
        return solrRepo.findOne(id);
    }

    public void save(final String id, final String orgId, final ObjectEnum category,
                     final String subject, final List<String> content) {
        if (content != null && !content.isEmpty()) {
            RecrutDocument rd = new RecrutDocument();
            rd.setCategory(category.toString());
            rd.setContent(content.stream().filter(s -> !(s.length() > 100 && !s.contains(" ")))
                    .collect(Collectors.toList()));
            rd.setId(id);
            rd.setOrgId(orgId);
            rd.setSubject(subject);
            if (CollectionUtils.isNotEmpty(rd.getContent())) {
                reIndexSolr(rd);
            }
        }
    }

    public void saveForClient(final String id, final String orgId, final ObjectEnum category,
                              final String subject, final List<String> content, final String name, final String descr) {
        if (content != null && !content.isEmpty()) {
            RecrutDocument rd = new RecrutDocument();
            rd.setCategory(category.toString());
            rd.setContent(content.stream().filter(s -> !(s.length() > 100 && !s.contains(" ")))
                    .collect(Collectors.toList()));
            rd.setId(id);
            rd.setOrgId(orgId);
            rd.setSubject(subject);
            String description = null;
            if (org.apache.commons.lang.StringUtils.isNotBlank(descr)) {
                description = Jsoup.parse(descr).text();
            }
            rd.setFullName(name);
            rd.setDescr(description);
            if (CollectionUtils.isNotEmpty(rd.getContent())) {
                reIndexSolr(rd);
            }
        }
    }

    public void saveForVacancy(final String id, final String orgId, final ObjectEnum category,
                               final String subject, final List<String> content, final String title, final String descr) {
        if (content != null && !content.isEmpty()) {
            RecrutDocument rd = new RecrutDocument();
            rd.setCategory(category.toString());
            rd.setContent(content.stream().filter(s -> !(s.length() > 100 && !s.contains(" ")))
                    .collect(Collectors.toList()));
            rd.setId(id);
            rd.setOrgId(orgId);
            rd.setSubject(subject);
            String description = null;
            if (org.apache.commons.lang.StringUtils.isNotBlank(descr)) {
                description = Jsoup.parse(descr).text();
            }
            rd.setDescr(description);
            rd.setHead(title);
            rd.setPosition(title);
            if (CollectionUtils.isNotEmpty(rd.getContent())) {
                reIndexSolr(rd);
            }
        }
    }

    public void saveForCandidate(final String id, final String orgId, final ObjectEnum category,
                                 final String subject, final List<String> content, final String title, final String coreSkills, final List<Skill> skill, final String descr, final String fullName) {
        if (content != null && !content.isEmpty()) {
            RecrutDocument rd = new RecrutDocument();
            rd.setCategory(category.toString());
            rd.setContent(content.stream().filter(s -> !(s.length() > 100 && !s.contains(" ")))
                    .collect(Collectors.toList()));
            rd.setId(id);
            rd.setOrgId(orgId);
            rd.setSubject(subject);
            String description = null;
            String fullSkill = null;
            if (org.apache.commons.lang.StringUtils.isNotBlank(descr)) {
                description = Jsoup.parse(descr).text();
            }
            String collect = null;
            if (CollectionUtils.isNotEmpty(skill)) {
                collect = skill.stream().map(n -> n.getName()).collect(Collectors.joining(" "));
            }
            if (org.apache.commons.lang.StringUtils.isNotBlank(coreSkills)) {
                fullSkill = Jsoup.parse(coreSkills).text() + collect;
            }
            rd.setHead(title);
            rd.setPosition(title);
            rd.setSkills(fullSkill);
            rd.setDescr(description);
            rd.setFullName(fullName);
            if (CollectionUtils.isNotEmpty(rd.getContent())) {
                reIndexSolr(rd);
            }

        }
    }

    public long count(final String orgId, final ObjectEnum category) {
        Criteria conditions = null;
        List<Criteria> criterias = new ArrayList<>();
        criterias.add(new Criteria("orgId").is(orgId));
        if (category != null) {
            criterias.add(new Criteria("category").is(category.toString()));
        }
        for (Criteria criteria : criterias) {
            if (conditions == null) {
                conditions = criteria;
            } else {
                conditions = conditions.and(criteria);
            }
        }
        SolrDataQuery sdq = new SimpleQuery(conditions);
        return template.count(sdq);
    }

    public List<RecrutDocument> getByOrgId(final String orgId) {
        return solrRepo.findByOrgId(orgId);
    }

    public void indexListDocument(final List<RecrutDocument> documentList) {
        solrRepo.save(documentList);
    }

    public void removeALL(final String orgId, final ObjectEnum category) {
        Criteria conditions = null;
        List<Criteria> criterias = new ArrayList<>();
        criterias.add(new Criteria("orgId").is(orgId));
        criterias.add(new Criteria("category").is(category.toString()));
        for (Criteria criteria : criterias) {
            if (conditions == null) {
                conditions = criteria;
            } else {
                conditions = conditions.and(criteria);
            }
        }
        SimpleQuery sdq = new SimpleQuery(conditions);
        sdq.addProjectionOnFields("id");
        PageRequest pageRequest = new PageRequest(0, 100000);
        sdq.setPageRequest(pageRequest);
        ScoredPage<RecrutDocument> recrutDocumentSearches = template.queryForPage(sdq, RecrutDocument.class);
        List<RecrutDocument> content = recrutDocumentSearches.getContent();
        solrRepo.delete(content);
    }

    public void reIndexSolr(final RecrutDocument rd) {
        documentIndexQueue.add(rd);
    } // ####################################################

    public void reIndexSolrNOW() {
        try {
            List<RecrutDocument> collect = documentIndexQueue.stream().collect(Collectors.toList());
            if (!collect.isEmpty()) {
                System.out.println("reIndexSolrNOW size " + collect.size());
                solrRepo.save(collect);
                documentIndexQueue.removeAll(collect);
            } else {
                System.out.println("reIndexSolrNOW EMPTY ");
            }
        } catch (Exception ex) {
            System.out.println("cron " + ex.getMessage());
            ex.printStackTrace();
            try {
                List<RecrutDocument> collect = documentIndexQueue.stream().collect(Collectors.toList());
                if (!collect.isEmpty()) {
                    System.out.println("reIndexSolrNOW size " + collect.size());
                    for (RecrutDocument recrutDocument : collect) {
                        try {
                            solrRepo.save(recrutDocument);
                        } catch (Exception ex2) {
                        } finally {
                            documentIndexQueue.remove(recrutDocument);
                        }
                    }
                } else {
                    System.out.println("reIndexSolrNOW EMPTY ");
                }
            } catch (Exception ex1) {
                System.out.println("cron " + ex1.getMessage());
                ex.printStackTrace();
            }
        }
    } // ####################################################

    @Scheduled(fixedDelay = 500)
    private void reIndexSolrCron() throws InterruptedException {
        List<RecrutDocument> collect = documentIndexQueue.stream().collect(Collectors.toList());
        try {
            if (!collect.isEmpty()) {
                logger.info("cron " + collect.stream().map(r -> r.getId()).collect(Collectors.joining(", ")));
                solrRepo.save(collect);
                documentIndexQueue.removeAll(collect);
            }
        } catch (Exception ex) {
            logger.info("Exception in reIndexSolrCron: " + ex.getMessage());
            if (ex.getMessage() != null && ex.getMessage().contains("Invalid content type:")) {
                documentIndexQueue.removeAll(collect);
                logger.info("Solved. Removed: " + collect.stream().map(r -> r.getId()).collect(Collectors.joining(", ")));
            }
            try {
                collect = documentIndexQueue.stream().collect(Collectors.toList());
                if (!collect.isEmpty()) {
                    logger.info("reIndexSolr size " + collect.size());
                    for (RecrutDocument recrutDocument : collect) {
                        try {
                            solrRepo.save(recrutDocument);
                        } catch (Exception ex2) {
                            loggerError.info("error " + recrutDocument.getId());
                            LogFactory.logException(ex2, loggerError);
                        } finally {
                            documentIndexQueue.remove(recrutDocument);
                        }
                    }

                } else {
                    logger.info("reIndexSolr EMPTY ");
                }
            } catch (Exception ex1) {
                loggerError.info("cron " + ex1.getMessage());
                LogFactory.logException(ex1, loggerError);
            }
        }
    } // ####################################################
}
