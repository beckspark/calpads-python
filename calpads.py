#%%
import asyncio
import functools
import os
import pyppeteer
from datetime import datetime
from kipp_schools import School, schools, get_school_from_string, create_school_variables
from typing import Tuple
from time import sleep

#%%
today = datetime.today()

#%%
async def select_school_calpads(page, school) -> pyppeteer.page.Page:
        selector_org_select = 'select[id="org-select"]' 
        if not await page.querySelectorAll(selector_org_select):
            await page.goto('https://calpads.org')
        try:
            await page.waitForSelector(selector_org_select)
            await page.select(selector_org_select,school.calpads_id)
        except:
            pass

        return page


#%%
async def login_calpads(creds, headless=True, dl_dir=None) -> Tuple[pyppeteer.browser.Browser, pyppeteer.page.Page]:
    """
    Asynchronous pyppeteer function that logs into the CALPADS web page. Takes a dictionary with the keys
    'Username' and 'Password', and uses those to log in. Returns the logged-in pyppeteer browser and page in
    a tuple, for use in other functions. Optionally takes 'headless' and 'dl_dir' arguments; 'dl_dir' sets the
    directory for any downloads during the page's session.
    """
    browser = await pyppeteer.launch(headless=headless)
    page = await browser.newPage()

    if dl_dir is not None:
        cdp = await page.target.createCDPSession()
        await cdp.send('Page.setDownloadBehavior', {
                'behavior': 'allow', 'downloadPath': dl_dir
            })


    await page.goto('https://calpads.org')
    await page.waitForSelector('button.btn.btn-primary',waitUntil='networkidle0')

    await page.type('#Username',creds['Username'])
    await page.type('#Password',creds['Password'])
    await page.click('#AgreementConfirmed')

    await page.click('button.btn.btn-primary')
    await page.waitForNavigation()

    return (browser, page)


#%%
async def upload_file_calpads(page, school, file_to_upload, report_type_str) -> pyppeteer.page.Page:
    """
    Asynchronous pyppeteer function that takes a pyppeteer page, a KIPP School object, a local file
    upload path, and a report type string ('SENR','CRSE', etc.). If the page passed to it is logged into
    an active CALPADS session, it will upload the file using the parameters passed to it,
    first using the select_school_calpads decorator to select the correct school for the file upload on the page.
    Returns the pyppeteer page after the upload is complete.
    """
    await select_school_calpads(page, school)

    await page.goto('https://www.calpads.org/FileSubmission/FileUpload')

    selector_report_type = 'select[name="FilesUploaded[0].FileType"]'
    await page.waitForSelector(selector_report_type)
    await page.select(selector_report_type, report_type_str)

    upload_elem = await page.querySelector('input[type="file"]')
    await upload_elem.uploadFile(file_to_upload)

    file_name = f"{school.short} {school.lea} {report_type_str} {today.strftime('%m.%d.%Y')}"
    selector_job_name = 'input[name="FilesUploaded[0].JobName"]'
    await page.waitForSelector(selector_job_name)
    await page.type(selector_job_name, file_name)

    selector_submit_file = 'button[value="Upload"]'
    await page.waitForSelector(selector_submit_file, waitUntil='networkidle0')
    await page.click(selector_submit_file)
    await page.waitForNavigation(timeout=0)

    print(f"{file_name} uploaded! :)")
    return page


#%%
async def generate_ods_calpads(page, school, report_type_str) -> pyppeteer.page.Page:
    """
    Asynch pyppeteer function that takes a logged-in CALPADS page, a school object, and a report type string ('SENR','SPRG',etc.)
    and returns that pyppeteer page after requesting a matching ODS extract on CALPADS
    """
    await select_school_calpads(page, school)

    await page.goto(f"https://www.calpads.org/Extract/ODSExtract?RecordType={report_type_str}")
    if report_type_str == 'SDEM':
        file_name = f"{report_type_str}_{school.short}_{school.lea}_Startdate_07012020"
        await page.click("input[name='EffectiveStartDate']")
        await page.type("input[name='EffectiveStartDate']","07012020")
        await page.click("input[name='EffectiveEndDate']")
        await page.type("input[name='EffectiveEndDate']","06302021")
    else:
        file_name = f"{report_type_str}_{school.short}_{school.lea}_20202021" 
        await page.waitForSelector("button[title='Move all']")
        await page.click("button[title='Move all']")
    try:
        await page.type("input[name='ExtractFileName']",f"{school.short}_{report_type_str}")
    except:
        await page.type("input[name='FileName']",file_name)
    await page.click("button[value='Request']", waitFor='networkidle0')

    return page


#%%
async def generate_ods_for_schools(schools_list, page, report_type_str) -> str:
    for school in schools_list:
        try:
            await generate_ods_calpads(page, school, report_type_str)
        except:
            await page.goto('https://calpads.org')
            await generate_ods_calpads(page, school, report_type_str)
        print(f"{school.name} {report_type_str} generated! :)")

    return print("Reports generated :)")


#%%
async def download_extract_calpads(page, school) -> pyppeteer.page.Page:
    """
    Function to download the most recently-generated extract on a school's CALPADS
    extract page. Takes a pyppeteer page object, a school object, and downloads
    the file, returning the page object once completed.
    """
    dl_selector = 'a[class="btn btn-default"]'
    await select_school_calpads(page, school)
    await page.goto("https://www.calpads.org/Extract")
    await page.waitForSelector(dl_selector)

    element = await page.querySelector(dl_selector)
    url = await page.evaluate('(element) => element.href', element)
    try:
        await page.goto(url)
        await page.waitForNavigation(timeout=0)

    # Pyppeteer has a quirk where it returns a PageError upon direct download
    # of a file. Here is some error handling to pass the module successfully
    # when the file is downloaded
    except pyppeteer.errors.PageError as page_error:
        if 'net::ERR_ABORTED' in str(page_error):
            pass
        else:
            raise page_error
    
    # TODO: add stuff for if you want to pipe it into pandas -- if get_df == True:
        

    return page


#%%
async def download_report_calpads(page, school, report_url) -> pyppeteer.page.Page:
    await select_school_calpads(page, school)
    await page.goto(report_url,waitUntil='domcontentloaded',timeout=0)

    frame_elem = await page.waitForSelector('iframe',waitUntil='domcontentloaded',timeout=0)
    frame = await frame_elem.contentFrame()

    select_selector = '#ReportViewer1_ctl08_ctl07_ddValue'
    #if report_num in selector_reports:
    await frame.waitForSelector(select_selector,timeout=0) 
    await frame.select(select_selector,'1')
    await frame.click('#ReportViewer1_ctl08_ctl00')
    await frame.waitForSelector('#ReportViewer1_ctl08_ctl00')
    await frame.click('#ReportViewer1_ctl08_ctl00')

    await frame.waitForSelector('#ReportViewer1_AsyncWait',hidden=True,timeout=0)
    await frame.evaluate('''() => {
        $find('ReportViewer1').exportReport('CSV');
    }''')
    # This is pure js code sent, via pyppeteer, to the page object in order to generate the csv and download
    sleep(3)
    return print(f"Done with {school.name}!")


#%%
async def update_lea_calpads_ic(page, school) -> pyppeteer.page.Page:
    lep_codes_dict = {
        'EO':'EO',
        'IFEP':'Not LEP',
        'TBD':'Pending',
        'EL':'LEP',
        'RFEP':'Exited LEP'
    }

    await generate_ods_calpads(page, school, 'SELA')
    await download_extract_calpads(page, school)

    return 
# %%
"""
browser, page = await login_calpads({'Username':'sbeck@kippsocal.org','Password':'Lambda1012'},headless=False,dl_dir=os.getcwd())
# %%
for school in schools:
    await download_extract_calpads(page, school)
    print(f"{school.name} extract downloaded! :)")

#%%
await browser.close()


#%%
txt_files = [file for file in os.listdir() if '.txt' in file]


# %%
txt_files
# %%
import pandas as pd
# %%
df = pd.concat([pd.read_table(file, delimiter='^', header=None)  for file in txt_files], ignore_index=True)
df

# %%
template_cols = pd.read_excel('./reporting_templates/SPRGFileTemplate.xlsx').columns.tolist()
# %%
df = df.iloc[:, :26] # 34 for SENR, 26 for SPRG
df.columns = template_cols
#df_1 = df_2[df_2['Enrollment Status Code'] == 20]
df_1 = df
# %%
df_1['Reporting LEA'] = '0' + df_1['Reporting LEA'].astype(str)
df_1['School of Attendance'] = df_1['Reporting LEA']

# %%
df_1['Record Type Code'] = 'SPRG'

#%%
df_frl = df_1[df_1['Education Program Code'].isin([181,182])]

#%%
df_frl_1 = df_frl[df_frl['Education Program Code'] == 181]
df_frl_2 = df_frl[df_frl['Education Program Code'] == 182]
df_frl_1['Education Program Code'] = 182
df_frl_2['Education Program Code'] = 181
df_frl_f = pd.concat([df_frl_1,df_frl_2], ignore_index=True)
df_frl_f['Education Program Membership End Date'] = '20210701'
#%%
df_frl_d = df_frl
df_frl_d['Transaction Type Code'] = 'D'
df_frl_d


#%%
from ic_reports import write_calpads_excel_file
from kipp_schools import get_school_from_string
#%%
for lea in df_frl_f['Reporting LEA'].unique().tolist():
    school = get_school_from_string(lea)
    df_s = df_frl_f[df_frl_f['Reporting LEA'] == lea]
    report_file_name = f"{school.short}_{school.lea}_Corrected_FRL_SPRG_{today.strftime('%m.%d.%Y')}.xlsx"
    write_calpads_excel_file('./reporting_templates/SPRGFileTemplate.xlsx', df_s, report_file_name)

#%%
report_xl_files = [file for file in os.listdir() if '.xlsx' in file]

#%%
try:
    await browser.close()
except:
    pass

browser, page = await login_calpads({'Username':'sbeck@kippsocal.org','Password':'Lambda1012'})
#%%
for file in ['KCCP_0121707_2.xlsx']:
    lea = file.split('_')[1]
    school = get_school_from_string(lea)
    await upload_file_calpads(page, school, file, 'SPRG')
    
#%%
await browser.close()
#%%
df_2[(df_2['Enrollment Status Code'] == 20) & (df_2['School of Attendance'] != 127670)]['SSID']

# %%
####
####

try:
    await browser.close()
except:
    pass
browser, page = await login_calpads({'Username':'sbeck@kippsocal.org','Password':'Lambda1012'},headless=False)
#%%
urls = ['https://www.calpads.org/Report/Snapshot/1_17_FRPM_EnglishLearnerFosterYouthCount',
'https://www.calpads.org/Report/Snapshot/1_2_EnrollmentPrimaryStatusStudentList',
'https://www.calpads.org/Report/Snapshot/1_18_FPRM_EnglishLearnerFosterYouthStudentList']
for school in schools:
    for url in urls:
        try:
            await browser.close()
        except:
            pass

        browser, page = await login_calpads({'Username':'sbeck@kippsocal.org','Password':'Lambda1012'},headless=False)
        await download_report_calpads(page, school, url)
# %%
# %%

# %%
"""