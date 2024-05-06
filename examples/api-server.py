from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
from JobSpy_AllSites import collect_jobs, create_tables, get_jobs, drop_tables, get_maxid
from flask import request

# this variable, db, will be used for all SQLAlchemy commands
db = SQLAlchemy()

app = Flask(__name__)

# change string to the name of your database; add path if necessary
db_name = 'jobmarket.db'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_name

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

# initialize the app with Flask-SQLAlchemy
db.init_app(app)
create_tables(app, db)

scheduler = APScheduler()

scheduler.init_app(app)

is_collecting = False
def check_if_collecting():
    return is_collecting
def set_is_collecting(val: bool):
    is_collecting = val


@scheduler.task('interval', id='my_job', seconds=1800)
def my_job():
    if check_if_collecting():
        print('Previous Collecting is not finished yet!')
        return
    print('Collecting new jobs...')
    set_is_collecting(True)
    try:
        collect_jobs(app, db)
    except Exception as e:
        print("")
    set_is_collecting(False)

scheduler.start()

@app.route("/drop")
def drop():
    drop_tables(app, db)
    return "droped all tables!"

@app.route("/run")
def run_collecting():
    create_tables(app, db)
    my_job()
    return "finished!"

@app.route("/maxid")
def maxid():
    maxid = get_maxid(db)
    return {'maxid':maxid}

@app.route("/jobs")
def new_jobs():
    try:
        args = request.args
        keyword = args['keyword']
        last_job_id = args['last_job_id']
        limit = args['limit']
        #print(f"/jobs: {keyword} {last_job_id} {limit}")
        new_jobs = get_jobs(keyword, last_job_id, limit, db)
        #print(f"new_jobs:{new_jobs}")
        return new_jobs
    except Exception as e:
        print(e)
        return '[]'

if __name__ == '__main__':
    # Debug/Development
    # app.run()
    # Production
    app.run(debug=False, host="0.0.0.0", port="5000")
