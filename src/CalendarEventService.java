package com.simargl.recrut.service;

import com.simargl.recrut.domain.*;
import com.simargl.recrut.enums.CalendarSourceEnum;
import com.simargl.recrut.enums.CalendarEventStatusEnum;
import com.simargl.recrut.enums.EventEntityTypeEnum;
import com.simargl.recrut.enums.ObjectEnum;
import com.simargl.recrut.repo.CalendarEventRepo;
import com.simargl.recrut.repo.CandidateRepo;
import com.simargl.recrut.repo.VacancyRepo;
import com.simargl.recrut.service.builder.GeneratorUtil;
import com.simargl.serverlib.logging.LogFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by MyMac on 6/14/16.
 */
@Service
public class CalendarEventService {
    @Qualifier("logFactory")
    @Autowired
    protected LogFactory logFactory;
    protected Logger logger;
    protected Logger loggerError;
    @Autowired
    protected CalendarEventRepo calendarEventRepo;
    @Autowired
    protected CandidateRepo candidateRepo;
    @Autowired
    protected VacancyRepo vacancyRepo;
    @Autowired
    protected ResponsibleService responsibleService;

    private static final ExecutorService threadPool = Executors.newCachedThreadPool();
    @PostConstruct
    private void postConstruct() {
        logger = LogFactory.getLogger(this.getClass().getSimpleName());
        loggerError = LogFactory.getLogger(this.getClass().getSimpleName() + "_ERROR");
    }

    public void addCalendarEvent(String source, String entity, String entityId,
                                 String userId, String orgId,
                                 String creatorId, String title,
                                 String description, Date eventDate) {
        CalendarEvent calendarEvent = new CalendarEvent();
        calendarEvent.setCalendarEventId(GeneratorUtil.generate());
        calendarEvent.setSource(source);
        calendarEvent.setEntity(entity);
        calendarEvent.setEntityId(entityId);
        calendarEvent.setUserId(userId);
        calendarEvent.setOrgId(orgId);
        calendarEvent.setCreatorId(creatorId);
        calendarEvent.setTitle(title);
        calendarEvent.setDescription(description);
        calendarEvent.setEventDate(eventDate);
        calendarEvent.setStatus(CalendarEventStatusEnum.N.toString());
        CalendarEvent save = calendarEventRepo.save(calendarEvent);
        logger.info("добаил евент"+save.getCalendarEventId());

    }

    public List<CalendarEvent> getCalendarEventsByEntityId(String entityId) {
        return calendarEventRepo.getCalendarEventByEntityId(entityId);
    }


    public void deleteCalendarEventsByEntityId(String entityId) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEventsByEntityId = getCalendarEventsByEntityId(entityId);
            if (CollectionUtils.isNotEmpty(calendarEventsByEntityId)) {
                calendarEventsByEntityId.stream().forEach(calendarEvent -> {
                    calendarEvent.setStatus(CalendarEventStatusEnum.DN.toString());
                    calendarEventRepo.save(calendarEvent);
                });
            }
        });

    }

    public void deleteCalendarEventsByEntityIdAndResponsibleId(String entityId, String responsibleId) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEvents = calendarEventRepo.getCalendarEventByEntityIdAndResponsibleId(entityId, responsibleId);
            CalendarEvent calendarEvent = calendarEvents.get(0);
            if (calendarEvent != null) {
                calendarEvent.setStatus(CalendarEventStatusEnum.DN.toString());
                calendarEventRepo.save(calendarEvent);
            }
        });
    }

//-------------------------TASK-----------------------


    public void addCalendarEventForTask(Task task, String savedTaskId, String orgId, String creatorId) {
        threadPool.execute(() -> {
            String entityName = calendarEventEntity(task);
            String title = calendarEventTitle(task);
            List<Responsible> responsibles = responsibleService.getResponsibleByObject(ObjectEnum.task, savedTaskId, orgId);
            responsibles.stream().forEach(responsible -> {
                addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                        entityName,
                        task.getTaskId(),
                        responsible.getPersonId(),
                        orgId,
                        creatorId,
                        title,
                        task.getText(),
                        task.getTargetDate()
                );
            });
        });
    }

    public void addCalendarEventResponsibleForTask(Task taskInBase, String responsibleId, String orgId, String modifierId) {
        threadPool.execute(() -> {
            String title = calendarEventTitle(taskInBase);
            String entityName = calendarEventEntity(taskInBase);
            if (taskInBase.getTargetDate() != null) {
                addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                        entityName,
                        taskInBase.getTaskId(),
                        responsibleId,
                        orgId,
                        modifierId,
                        title,
                        taskInBase.getText(),
                        taskInBase.getTargetDate()
                );
            }
        });
    }

    public String calendarEventEntity(Task task) {

        String entityName = null;
        if (task.getVacancyId() == null && task.getCandidateId() != null) {
            entityName = EventEntityTypeEnum.candidateTask.toString();
        } else if (task.getVacancyId() != null && task.getCandidateId() == null) {
            entityName = EventEntityTypeEnum.vacancyTask.toString();
        }
        return entityName;
    }

    public String calendarEventTitle(Task task) {
        String title = null;
        if (task.getVacancyId() == null && task.getCandidateId() != null) {
            String candidateName = candidateRepo.getCandidateName(task.getCandidateId());
            title = "Task for the candidate " + candidateName;
        } else if (task.getVacancyId() != null && task.getCandidateId() == null) {
            String vacancyPosition = vacancyRepo.getPosition(task.getVacancyId());
            title = "Task for the vacancy " + vacancyPosition;
        }
        return title;
    }

    public void editCalendarEventForTask(Task task) {
        threadPool.execute(() -> {
            String title = calendarEventTitle(task);
            List<CalendarEvent> calendarEvents = getCalendarEventsByEntityId(task.getTaskId());
            calendarEvents.stream().forEach(calendarEvent -> {
                calendarEvent.setTitle(title);
                calendarEvent.setTitle(task.getText());
                calendarEvent.setEventDate(task.getTargetDate());
                calendarEvent.setStatus(CalendarEventStatusEnum.C.toString());
                calendarEventRepo.save(calendarEvent);
            });
        });
    }

    public void editCalendarEventDateForTask(Task taskInBase, Date targetDate) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEvents = getCalendarEventsByEntityId(taskInBase.getTaskId());
            calendarEvents.stream().forEach(calendarEvent -> {
                calendarEvent.setEventDate(targetDate);
                calendarEvent.setStatus(CalendarEventStatusEnum.C.toString());
                calendarEventRepo.save(calendarEvent);
            });
        });
    }
//------------------------------VACANCY---------------------------

    public void addCalendarEventForVacancy(String vacancyId, String orgId, String creator, String vecancyPosition, Date dateFinish) {
        threadPool.execute(() -> {
            List<Responsible> responsibles = responsibleService.getResponsibleByObject(ObjectEnum.vacancy, vacancyId, orgId);
            System.out.println(responsibles.size());
            responsibles.stream().forEach(responsible -> {
                addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                        EventEntityTypeEnum.vacancy.toString(),
                        vacancyId,
                        responsible.getPersonId(),
                        orgId,
                        creator,
                        vecancyPosition,
                        null,
                        dateFinish
                );
            });
        });
    }

    public void addCalendarEventResponsibleForVacancy(Vacancy vacancyOld, String responsibleId, String orgId, String modyfier) {
        threadPool.execute(() -> {
            addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                    EventEntityTypeEnum.vacancy.toString(),
                    vacancyOld.getVacancyId(),
                    responsibleId,
                    orgId,
                    modyfier,
                    vacancyOld.getPosition(),
                    null,
                    vacancyOld.getDateFinish()
            );
        });
    }


    public void editCalendarEventForVacancy(Vacancy vacancy) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEvents = getCalendarEventsByEntityId(vacancy.getVacancyId());
            calendarEvents.stream().forEach(calendarEvent -> {
                calendarEvent.setEventDate(vacancy.getDateFinish());
                calendarEvent.setTitle(vacancy.getPosition());
                calendarEvent.setStatus(CalendarEventStatusEnum.C.toString());
                calendarEventRepo.save(calendarEvent);
            });
        });
    }


//------------------------------INTERVIEW---------------------------

    public void addCalendarEventForInterview(Interview interview, Vacancy vacancy, String orgId, String modifier) {
        threadPool.execute(() -> {
            List<Responsible> responsibles = responsibleService.getResponsibleByObject(ObjectEnum.vacancy, vacancy.getVacancyId(), orgId);
            responsibles.stream().forEach(responsible -> {
                addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                        EventEntityTypeEnum.interview.toString(),
                        interview.getInterviewId(),
                        responsible.getPersonId(),
                        orgId,
                        modifier,
                        vacancy.getPosition(),
                        interview.getComment(),
                        interview.getDateInterview()
                );
            });
        });
    }

    public void addCalendarEventForInterviewWithDeleting(Interview interview, Vacancy vacancy, String orgId, String modifier) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEventsByEntityId = getCalendarEventsByEntityId(interview.getInterviewId());
            if (CollectionUtils.isNotEmpty(calendarEventsByEntityId)) {
                calendarEventsByEntityId.stream().forEach(calendarEvent -> {
                    calendarEvent.setStatus(CalendarEventStatusEnum.DN.toString());
                    calendarEventRepo.save(calendarEvent);
                });
            }
            List<Responsible> responsibles = responsibleService.getResponsibleByObject(ObjectEnum.vacancy, vacancy.getVacancyId(), orgId);
            responsibles.stream().forEach(responsible -> {
                addCalendarEvent(CalendarSourceEnum.googleCalendar.toString(),
                        EventEntityTypeEnum.interview.toString(),
                        interview.getInterviewId(),
                        responsible.getPersonId(),
                        orgId,
                        modifier,
                        vacancy.getPosition(),
                        interview.getComment(),
                        interview.getDateInterview()
                );
            });
        });
    }

    public void editCalendarEventForInterview(Interview interview, Date date) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEvents = getCalendarEventsByEntityId(interview.getInterviewId());
            calendarEvents.stream().forEach(calendarEvent -> {
                calendarEvent.setEventDate(date);
                calendarEvent.setStatus(CalendarEventStatusEnum.C.toString());
                calendarEventRepo.save(calendarEvent);
            });
        });
    }

    public void editCalendarEventCommentForInterviw(Interview interview, String comment) {
        threadPool.execute(() -> {
            List<CalendarEvent> calendarEvents = getCalendarEventsByEntityId(interview.getInterviewId());
            calendarEvents.stream().forEach(calendarEvent -> {
                calendarEvent.setDescription(comment);
                calendarEvent.setStatus(CalendarEventStatusEnum.C.toString());
                calendarEventRepo.save(calendarEvent);
            });
        });
    }
}

