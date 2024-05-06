from ..jobspy import scrape_jobs
import pandas as pd


def test_linkedin():
    result = scrape_jobs(
        site_name=["linkedin"],
        search_term="rust",
        location="United States",
        results_wanted=20,
        hours_old=72, # (only Linkedin/Indeed is hour specific, others round up to days old)
        country_indeed='USA',  # only needed for indeed / glassdoor
        # linkedin_fetch_description=True # get full description and direct job url for linkedin (slower)
    )

    file_name = f"./1.csv"
    #print(f"outputted to {file_name}")
    result.to_csv(file_name, index=False)
    

    
