package com.simargl.recrut.service.calendar;

import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.CalendarScopes;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.CalendarList;
import com.google.api.services.calendar.model.CalendarListEntry;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventDateTime;
import com.simargl.recrut.domain.*;
import com.simargl.recrut.enums.CalendarEventStatusEnum;
import com.simargl.recrut.enums.EventEntityTypeEnum;
import com.simargl.recrut.enums.GoogleCalendarStatusEnum;
import com.simargl.recrut.repo.CalendarEventRepo;
import com.simargl.recrut.repo.GoogleCalendarRepo;
import com.simargl.recrut.service.CandidateService;
import com.simargl.recrut.service.PreferenceService;
import com.simargl.recrut.service.VacancyService;
import com.simargl.recrut.service.builder.GeneratorUtil;
import com.simargl.serverlib.logging.LogFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class GoogleCalendarService {

    @Qualifier("logFactory")
    @Autowired
    protected LogFactory logFactory;
    protected Logger logger;
    protected Logger loggerError;
    @Autowired
    protected GoogleCalendarRepo googleCalendarRepo;
    @Autowired
    protected VacancyService vacancyService;
    @Autowired
    protected CandidateService candidateService;
    @Autowired
    protected CalendarEventRepo calendarEventRepo;
    @Autowired
    protected PreferenceService preferenceService;


    protected static final String APP_NAME = "CleverStaff";
    protected static final String USER = "me";
    protected static final String calendarName = "CleverStaff events";
    protected static GoogleClientSecrets clientSecrets;
    protected static final JsonFactory jsonFactory = new JacksonFactory();
    protected static final HttpTransport httpTransport = new NetHttpTransport();
    protected String redirectUri;
    protected GoogleAuthorizationCodeFlow googleAuthorizationCodeFlow;


    public GoogleCalendarService(String configPath) {
        logger = LogFactory.getLogger(this.getClass().getSimpleName());
        loggerError = LogFactory.getLogger(this.getClass().getSimpleName() + "_ERROR");
        try {
            clientSecrets = GoogleClientSecrets.load(jsonFactory, new FileReader(configPath));
            redirectUri = clientSecrets.getDetails().getRedirectUris().get(0);
            googleAuthorizationCodeFlow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, jsonFactory, clientSecrets,
                    Collections.singleton(CalendarScopes.CALENDAR))
                    .setAccessType("offline").setApprovalPrompt("force").build();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public GoogleCalendar createGoogleCalendar(final String accessToken, final String userId, final String orgId) throws IOException {
        String token = getToken(accessToken);
        Calendar calendarService = createCalendarService(token);
        String calendarId = getCalendarId(calendarService, true);
        List<GoogleCalendar> calendars = googleCalendarRepo.findByUserIdAndOrgId(userId, orgId);
        GoogleCalendar googleCalendar = new GoogleCalendar();
        if (CollectionUtils.isEmpty(calendars)) {
            googleCalendar.setCalendarId(calendarId);
            googleCalendar.setDc(new Date());
            googleCalendar.setToken(token);
            googleCalendar.setGcId(GeneratorUtil.generate());
            googleCalendar.setOrgId(orgId);
            googleCalendar.setUserId(userId);
            googleCalendar.setStatus(GoogleCalendarStatusEnum.ok.toString());
        } else {
            googleCalendar = calendars.get(0);
            googleCalendar.setToken(token);
            googleCalendar.setCalendarId(calendarId);
            googleCalendar.setStatus(GoogleCalendarStatusEnum.ok.toString());
        }
        GoogleCalendar save = googleCalendarRepo.save(googleCalendar);
        return save;
    }

    public void deleteGoogleCalendar(final String userId, final String orgId) throws IOException {
        List<GoogleCalendar> calendars = googleCalendarRepo.findByUserIdAndOrgId(userId, orgId);
        if (CollectionUtils.isNotEmpty(calendars)) {
            googleCalendarRepo.deleteGoogleCalendarByUserIdAndOrgId(userId, orgId);
        }
    }

    public GoogleCalendar getGoogleCalendar(final String userId, final String orgId) {
        List<GoogleCalendar> googleCalendars = googleCalendarRepo.getGoogleCalendarByUserIdAndOrgId(userId, orgId);
        if (CollectionUtils.isNotEmpty(googleCalendars)) {

            return googleCalendars.get(0);
        }

        return null;
    }

    public Calendar createCalendarServiceForUser(final String userId, final String orgId) throws IOException {
        GoogleCredential credential = new GoogleCredential.Builder().setClientSecrets(clientSecrets).setJsonFactory(jsonFactory)
                .setTransport(httpTransport).build();
        List<GoogleCalendar> calendars = googleCalendarRepo.findByUserIdAndOrgId(userId, orgId);
        if (CollectionUtils.isNotEmpty(calendars)) {
            credential.setRefreshToken(calendars.get(0).getToken());
        }
        Calendar calendar = new Calendar.Builder(httpTransport, jsonFactory, credential).setApplicationName(APP_NAME).build();
        String calendarId = getCalendarId(calendar, true);
        if (!calendarId.equalsIgnoreCase(calendars.get(0).getCalendarId())) {
            calendars.get(0).setCalendarId(calendarId);
            googleCalendarRepo.save(calendars.get(0));
        }
        return calendar;
    }

    /**
     * create calendar if need
     *
     * @param calendar
     * @return
     * @throws IOException
     */


    public String getCalendarId(final Calendar calendar, final boolean createIfNone) throws IOException {
        CalendarList calendarList = calendar.calendarList().list().execute();
        for (CalendarListEntry item : calendarList.getItems()) {
            if (item.getSummary().equalsIgnoreCase(calendarName)) {
                return item.getId();
            }
        }
        if (createIfNone) {
            com.google.api.services.calendar.model.Calendar calendarNew = new com.google.api.services.calendar.model.Calendar();
            calendarNew.setSummary(calendarName);
            calendarNew = calendar.calendars().insert(calendarNew).execute();
            return calendarNew.getId();
        } else {
            return null;
        }
    }

    public String getToken(String code) throws IOException {
//        System.out.println(redirectUri);
        GoogleTokenResponse response = googleAuthorizationCodeFlow.newTokenRequest(code).setRedirectUri(redirectUri).execute();
        return response.getRefreshToken();
    }


    public Calendar createCalendarService(final String token) throws IOException {
        GoogleCredential credential = new GoogleCredential.Builder().setClientSecrets(clientSecrets).setJsonFactory(jsonFactory)
                .setTransport(httpTransport).build();
        credential.setRefreshToken(token);
        return new Calendar.Builder(httpTransport, jsonFactory, credential).setApplicationName(APP_NAME).build();
    }


    public Date plusHour(Date date, Integer hourCount) {
        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        ldt.plusHours(hourCount);
        return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    }


    public Event makeEvent(String summery, String description, Date startDate) {
        Event event = new Event();
        event.setSummary(summery);
        event.setDescription(description);
        DateTime startDateTime = new DateTime(startDate);
        EventDateTime start = new EventDateTime()
                .setDateTime(startDateTime);
        event.setStart(start);
        DateTime endDateTime = new DateTime(plusHour(startDate, 2));
        EventDateTime end = new EventDateTime()
                .setDateTime(endDateTime);
        event.setEnd(end);
        return event;
    }

    @Scheduled(cron = "0 */1 * * * *") //каждые 1 минутa
    public void saveGcEvent() {
        if (preferenceService.isActiveCron()) {
            logger.info("Scheduled");
            List<CalendarEvent> calendarEventWhereUserHaveTokenWithStatusDN = calendarEventRepo.getCalendarEventWhereUserHaveTokenWithStatus("DN");
            if (CollectionUtils.isNotEmpty(calendarEventWhereUserHaveTokenWithStatusDN)) {
                Set<String> userIdsWithStatusDN = calendarEventWhereUserHaveTokenWithStatusDN.stream().map(q -> q.getUserId()).collect(Collectors.toSet());
                List<GoogleCalendar> calendarsDN = googleCalendarRepo.findByUserId(userIdsWithStatusDN);
                Map<String, GoogleCalendar> collectDN = calendarsDN.stream().collect(Collectors.toMap(GoogleCalendar::getUserId, g -> g, (g1, g2) -> g1));
                calendarEventWhereUserHaveTokenWithStatusDN.stream().forEach(calendarEvent -> {
                    if (calendarEvent.getGcEventId() != null) {
                        String userId = calendarEvent.getUserId();
                        String orgId = calendarEvent.getOrgId();
                        GoogleCalendar googleCalendar = collectDN.get(calendarEvent.getUserId());
                        Calendar calendarServiceForUser = null;
                        try {
                            calendarServiceForUser = createCalendarServiceForUser(userId, orgId);
                            calendarServiceForUser.events().delete(googleCalendar.getCalendarId(), calendarEvent.getGcEventId()).execute();
                            calendarEvent.setStatus(CalendarEventStatusEnum.DY.toString());
                            calendarEventRepo.save(calendarEvent);
                        } catch (com.google.api.client.auth.oauth2.TokenResponseException e1) {
                            googleCalendar.setStatus(GoogleCalendarStatusEnum.badToken.toString());
                            googleCalendarRepo.save(googleCalendar);
                        } catch (IOException e2) {
                            googleCalendar.setStatus(GoogleCalendarStatusEnum.error.toString());
                            googleCalendarRepo.save(googleCalendar);
                        }
                    } else {
                        calendarEvent.setStatus(CalendarEventStatusEnum.DY.toString());
                        calendarEventRepo.save(calendarEvent);
                    }
                });
            }
            List<CalendarEvent> calendarEventWhereUserHaveTokenWithStatusC = calendarEventRepo.getCalendarEventWhereUserHaveTokenWithStatus("C");
            if (CollectionUtils.isNotEmpty(calendarEventWhereUserHaveTokenWithStatusC)) {
                Set<String> userIdsWithStatusC = calendarEventWhereUserHaveTokenWithStatusC.stream().map(q -> q.getUserId()).collect(Collectors.toSet());
                List<GoogleCalendar> calendarsC = googleCalendarRepo.findByUserId(userIdsWithStatusC);
                Map<String, GoogleCalendar> collectC = calendarsC.stream().collect(Collectors.toMap(GoogleCalendar::getUserId, g -> g, (g1, g2) -> g1));
                calendarEventWhereUserHaveTokenWithStatusC.stream().forEach(calendarEvent -> {
                    if (calendarEvent.getGcEventId() != null) {
                        String userId = calendarEvent.getUserId();
                        String orgId = calendarEvent.getOrgId();
                        GoogleCalendar googleCalendar = collectC.get(calendarEvent.getUserId());
                        Calendar calendarServiceForUser = null;
                        try {
                            calendarServiceForUser = createCalendarServiceForUser(userId, orgId);
                            Event event = calendarServiceForUser.events().get(googleCalendar.getCalendarId(), calendarEvent.getGcEventId()).execute();
                            event.setSummary(calendarEvent.getTitle());
                            event.setDescription(calendarEvent.getDescription());
                            DateTime startDateTime = new DateTime(calendarEvent.getEventDate());
                            EventDateTime start = new EventDateTime()
                                    .setDateTime(startDateTime);
                            event.setStart(start);
                            DateTime endDateTime = new DateTime(plusHour(calendarEvent.getEventDate(), 2));
                            EventDateTime end = new EventDateTime()
                                    .setDateTime(endDateTime);
                            event.setEnd(end);
                            calendarServiceForUser.events().update(googleCalendar.getCalendarId(), event.getId(), event).execute();
                            calendarEvent.setStatus(CalendarEventStatusEnum.Y.toString());
                            calendarEventRepo.save(calendarEvent);
                        } catch (com.google.api.client.auth.oauth2.TokenResponseException e1) {
                            googleCalendar.setStatus(GoogleCalendarStatusEnum.badToken.toString());
                            googleCalendarRepo.save(googleCalendar);
                        } catch (IOException e2) {
                            googleCalendar.setStatus(GoogleCalendarStatusEnum.error.toString());
                            googleCalendarRepo.save(googleCalendar);
                        }
                    } else {
                        calendarEvent.setStatus(CalendarEventStatusEnum.N.toString());
                        calendarEventRepo.save(calendarEvent);
                    }
                });
            }
            List<CalendarEvent> calendarEventWhereUserHaveTokenWithStatusN = calendarEventRepo.getCalendarEventWhereUserHaveTokenWithStatus("N");
            if (CollectionUtils.isNotEmpty(calendarEventWhereUserHaveTokenWithStatusN)) {
                System.out.println("empty" + calendarEventWhereUserHaveTokenWithStatusN.size());
                logger.info("calendarEventWhereUserHaveTokenWithStatusN" + calendarEventWhereUserHaveTokenWithStatusN.size());
                Set<String> userIdsWithStatusN = calendarEventWhereUserHaveTokenWithStatusN.stream().map(q -> q.getUserId()).collect(Collectors.toSet());
                System.out.println("ids" + userIdsWithStatusN.size());
                logger.info("userIdsWithStatusN" + userIdsWithStatusN.size());
                List<GoogleCalendar> calendarsN = googleCalendarRepo.findByUserId(userIdsWithStatusN);
                System.out.println("calendarsN" + calendarsN.size());
                logger.info("calendarsN" + calendarsN.size());
                Map<String, GoogleCalendar> collectN = calendarsN.stream().collect(Collectors.toMap(GoogleCalendar::getUserId, g -> g, (g1, g2) -> g1));
                logger.info("collectN" + collectN.size());
                calendarEventWhereUserHaveTokenWithStatusN.stream().forEach(calendarEvent -> {
                    if (calendarEvent.getEntity().equals(EventEntityTypeEnum.interview.toString())) {
                        String summery = "Interview on vacancy " + calendarEvent.getTitle();
                        String description = calendarEvent.getDescription();
                        Date startDate = calendarEvent.getEventDate();
                        String userId = calendarEvent.getUserId();
                        String orgId = calendarEvent.getOrgId();
                        Event event = makeEvent(summery, description, startDate);
                        executeEvent(calendarEvent, event, userId, orgId, collectN);
                    } else if (calendarEvent.getEntity().equals(EventEntityTypeEnum.vacancy.toString())) {
                        Date startDate = calendarEvent.getEventDate();
                        String userId = calendarEvent.getUserId();
                        String orgId = calendarEvent.getOrgId();
                        LocalDateTime sd = LocalDateTime.ofInstant(startDate.toInstant(), ZoneId.systemDefault());
                        String summery = "Term for a vacancy " + calendarEvent.getTitle() + " - " + sd;
                        Event event = makeEvent(summery, null, startDate);
                        executeEvent(calendarEvent, event, userId, orgId, collectN);
                    } else if (calendarEvent.getEntity().equals(EventEntityTypeEnum.candidateTask.toString()) || calendarEvent.getEntity().equals(EventEntityTypeEnum.vacancyTask.toString())) {
                        String summery = calendarEvent.getTitle();
                        Date startDate = calendarEvent.getEventDate();
                        String userId = calendarEvent.getUserId();
                        String orgId = calendarEvent.getOrgId();
                        Event event = makeEvent(summery, null, startDate);
                        executeEvent(calendarEvent, event, userId, orgId, collectN);
                    }
                });
            }

        }
    }

    public void executeEvent(CalendarEvent calendarEvent, Event event, String userId, String orgId, Map<String, GoogleCalendar> collect) {
        Calendar calendarServiceForUser;
        GoogleCalendar googleCalendar = collect.get(calendarEvent.getUserId());
        try {
            calendarServiceForUser = createCalendarServiceForUser(userId, orgId);
            Event execute = calendarServiceForUser.events().insert(googleCalendar.getCalendarId(), event).execute();
            logger.info("executeEvent" + execute.getSummary());
            calendarEvent.setStatus(CalendarEventStatusEnum.Y.toString());
            calendarEvent.setGcEventId(execute.getId());
            calendarEventRepo.save(calendarEvent);
        } catch (com.google.api.client.auth.oauth2.TokenResponseException e1) {
            googleCalendar.setStatus(GoogleCalendarStatusEnum.badToken.toString());
            googleCalendarRepo.save(googleCalendar);
        } catch (IOException e2) {
            googleCalendar.setStatus(GoogleCalendarStatusEnum.error.toString());
            googleCalendarRepo.save(googleCalendar);
        }
    }
}
