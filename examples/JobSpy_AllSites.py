from jobspy import scrape_jobs
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy import inspect
import json
from sqlalchemy import func 
from datetime import datetime

Base = declarative_base()

def collect_jobs(app, db: SQLAlchemy):
    jobs: pd.DataFrame = collect_site_jobs()
    #print(f"jobs: {jobs}")
    save_distinct_jobs(app, jobs, db)
    return []

def collect_site_jobs():
    try:
        jobs: pd.DataFrame = scrape_jobs(
            site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor"],
            #site_name=["linkedin"],
            search_term="rust",
            location="United States",
            is_remote=True,
            easy_apply=False,
            job_type="fulltime",
            date_posted="24hr", # past month, past week, 24hr
            results_wanted=1000000,  # be wary the higher it is, the more likey you'll get blocked (rotating proxy should work tho)
            country_indeed="USA",
            offset=0  # start jobs from an offset (use if search failed and want to continue)
        )
        return jobs
    except Exception as e:
        print(f"error")
        print(str(e))

def save_distinct_jobs(app, jobs, db):
    #print(f"save_distinct_jobs:{jobs}")
    try:
        for job_index in jobs.index:
            search_term = "rust"
            #search_term = jobs.at[job_index, "keyword"]
            title = jobs.at[job_index, "title"]
            #print("title good")
            #applicants = jobs.at[job_index, "applicants"]
            applicants = "None"
            #print("applicants good")
            job_url = jobs.at[job_index, "job_url"]
            #print("job_url good")
            apply_url = jobs.at[job_index, "job_url_direct"]
            #print("job_url_direct good")
            site = jobs.at[job_index, "site"]
            #print("site good")
            location = jobs.at[job_index, "location"]
            #print("location good")
            company = jobs.at[job_index, "company"]
            #print("company good")
            description = jobs.at[job_index, "description"]
            #print("description good")

            
            #print(f"job_index:{job_index} title: {title}\n")

            with app.app_context():
                exists = db.session.query(JobTable.id).filter_by(job_url=job_url).first() is not None
                #print(f"exists: {exists}")
                if not exists:
                    newJob = JobTable(
                        title = title,
                        keyword = search_term,
                        applicants = applicants,
                        job_url = job_url,
                        apply_url = apply_url,
                        site = site,
                        location = location,
                        company = company,
                        description = description,
                    )

                    #print(f"newJobs: {newJob}")

                    db.session.add(newJob)   
                    db.session.commit()
    except Exception as e:
        return
    
    return

class JobTable(Base):
    __tablename__ = 'job'
    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(255))
    title = Column(String(255))
    applicants = Column(String(255))
    job_url = Column(Text)
    apply_url = Column(Text)
    site = Column(String(255))
    location = Column(String(255))
    company = Column(String(255))
    description = Column(Text)
    time_collected = Column(String(255), server_default=datetime.utcnow().strftime('%Y.%m.%d.'))

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    def as_json_str(self):
        return json.dumps(self.as_dict())

def create_tables(app, db):
    with app.app_context():
        if not inspect(db.engine).has_table("Job"):
            JobTable.__table__.create(db.engine)
        #Base.metadata.tables['job'].create(db.engine)
def drop_tables(app, db):
    with app.app_context():
        if inspect(db.engine).has_table("Job"):
            JobTable.__table__.drop(db.engine)
def get_jobs(keyword, last_job_id, limit, db):
    jobs = db.session.query(JobTable).filter(JobTable.id > last_job_id, JobTable.keyword == keyword).limit(limit).all()
    return json.dumps([ job.as_dict() for job in jobs ])
def get_maxid(db):
    maxid = db.session.query(func.max(JobTable.id)).scalar()
    return maxid