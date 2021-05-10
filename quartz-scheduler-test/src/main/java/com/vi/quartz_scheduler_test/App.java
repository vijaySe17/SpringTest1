package com.vi.quartz_scheduler_test;

import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		SchedulerFactory schedFact = new StdSchedulerFactory();
		try {

			final Scheduler sched = schedFact.getScheduler();

			JobDetail job = JobBuilder.newJob(SimpleJob.class).withIdentity("myJob", "group1")
					.usingJobData("jobSays", "Hello World!").usingJobData("myFloatValue", 3.141f).build();

			Trigger trigger = TriggerBuilder.newTrigger().withIdentity("myTrigger", "group1").startNow()
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(40).repeatForever())
					.build();

			JobDetail jobA = JobBuilder.newJob(JobA.class).withIdentity("jobA", "group2").build();

			JobDetail jobB = JobBuilder.newJob(JobB.class).withIdentity("jobB", "group2").build();

			Trigger triggerA = TriggerBuilder.newTrigger().withIdentity("tA", "group2").startNow()
					.withPriority(15)
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(40).repeatForever())
					.build();

			Trigger triggerB = TriggerBuilder.newTrigger().withIdentity("tB", "group2").startNow()
					.withPriority(10)
					.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(20).repeatForever())
					.build();

			sched.scheduleJob(job, trigger);
			sched.scheduleJob(jobA, triggerA);
			sched.scheduleJob(jobB, triggerB);
			sched.start();
//			sched.getListenerManager().

			new Thread() {
				public void run() {
					Scanner sc = new Scanner(System.in);
					while (true) {

						System.out.println("1. Details All");
						System.out.println("2. Running Jobs");
						System.out.println("3. Pause a Job");
						System.out.println("4. Reschedule a Job");
						System.out.println("S. Safe Exit");
						System.out.println("Q. Exit");
						System.out.print("Enter Op:");
						String in = sc.next();
						try {
							if ("1".equals(in)) {
								Set<JobKey> jobKeys = sched.getJobKeys(GroupMatcher.anyJobGroup());
								jobKeys.stream().forEach(s-> System.out.println(s.getName()+"->"+s.getGroup()));
								System.out.println(jobKeys);
							}
							if("2".equals(in)) {
								List<JobExecutionContext> currentlyExecutingJobs = sched.getCurrentlyExecutingJobs();
								System.out.println(currentlyExecutingJobs.stream().map(s->s.getJobDetail().getKey().getName()).collect(Collectors.toList()));
								//System.out.println(currentlyExecutingJobs);
							}
							if("3".equals(in)) {
								System.out.println("Enter Job Name: ");
								String jobName = sc.next();
								sched.pauseJob(JobKey.jobKey(jobName, groupName(jobName, sched)));
								System.out.println(jobName+" paused.");
							}
							if("4".equals(in)) {
								System.out.println("Enter Job Name: ");
								String jobName = sc.next();
								List<? extends Trigger> t = sched.getTriggersOfJob(JobKey.jobKey(jobName, groupName(jobName, sched)));
								System.out.println(t);
								if(t != null && !t.isEmpty()) {
									Trigger t0 = t.get(0);
									System.out.println("Enter New Time(in Sec): ");
									int time = Integer.parseInt(sc.next());
									Trigger t2 = TriggerBuilder.newTrigger()
											.forJob(t0.getJobKey())
											.withPriority(t0.getPriority())
											.withDescription(t0.getDescription())
											.startNow()
											.withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(time).repeatForever())
									.build();
									sched.rescheduleJob(t0.getKey(), t2);
									System.out.println(jobName+" rescheduled Job.");
								}else {
									System.out.println(jobName+" Not found rescheduled Job.");
								}
								
							}
							if ("q".equalsIgnoreCase(in)) {
								sched.shutdown();
								break;
							}
							if ("s".equalsIgnoreCase(in)) {
								sched.shutdown(true);
								break;
							}
						} catch (SchedulerException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					sc.close();
				}
			}.start();

		} catch (SchedulerException e) {
			e.printStackTrace();
		}

	}

	public static class SimpleJob implements Job {

		public void execute(JobExecutionContext context) throws JobExecutionException {
			JobDataMap dataMap = context.getJobDetail().getJobDataMap();

			String jobSays = dataMap.getString("jobSays");
			float myFloatValue = dataMap.getFloat("myFloatValue");

			System.out.println("Job says: " + jobSays + ", and val is: " + myFloatValue);
		}
	}

	public static class JobA implements Job {

		public void execute(JobExecutionContext arg0) throws JobExecutionException {
			System.out.println("This is the job A Start");
			sleep(10000);
			System.out.println("This is the job A End");
			
		
		}
	}

	public static class JobB implements Job {

		public void execute(JobExecutionContext arg0) throws JobExecutionException {
			System.out.println("This is the job B Start");
			sleep(20000);
			System.out.println("This is the job B End");
		}
	}
	
	static void sleep(long millis) {
		try {
			Thread.currentThread().sleep(millis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String groupName(String jobName, Scheduler sc) throws SchedulerException {
		return sc.getJobKeys(GroupMatcher.anyJobGroup()).stream().filter(s->jobName.equals(s.getName())).findAny().get().getGroup();
	}

}
