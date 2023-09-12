import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import gspread2df as g2d
import re
from apify_client import ApifyClient
import streamlit as st
import streamlit_authenticator as stauth
import requests
from streamlit_lottie import st_lottie_spinner
import time
import random
from google.oauth2.service_account import Credentials
import yaml
from yaml.loader import SafeLoader


favicon = 'https://polimata.ai/wp-content/uploads/2023/07/favicon-32x32-1.png'
st.set_page_config(
    page_title="Polímata.AI",
    page_icon=favicon,
    initial_sidebar_state="expanded"
)

hide_default_format = """
       <style>
       #MainMenu {visibility: hidden; }
       footer {visibility: hidden;}
       </style>
       """
st.markdown(hide_default_format, unsafe_allow_html=True)

# st.image("https://i.imgur.com/XQ0ePg2.png", use_column_width='auto')
st.title(':factory: Procesador de Leads')
st.caption(':turtle: V1.01 by Polímata.AI')

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)
name, authentication_status, username = authenticator.login('Login', 'sidebar')

if authentication_status == False:
    st.error('Username o contraseña incorrectos')
if authentication_status == None:
    st.warning("Ingresa tu contraseña")
if authentication_status== True:
    name_user=name
    st.sidebar.title(f'Hola {name}!')
    authenticator.logout('Logout', "sidebar")
################################################################
    col1, col2 =st.columns([1,3])
    col1.subheader('Elige el output de descargas de Apollo')
    st.divider()
    
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    
    skey = st.secrets["gcp_service_account"]
    credentials = Credentials.from_service_account_info(
        skey,
        scopes=scopes,
    )
    client = gspread.authorize(credentials)
    
    def load_data(url, sheet_name):
        sh = client.open_by_url(url)
        df = pd.DataFrame(sh.worksheet(sheet_name).get_all_records())
        return df
    
    runButton2= col2.empty()
    uploaded_file = col2.empty()
    APOLLO_CSV = uploaded_file.file_uploader('Carga el .CSV de Apollo:', type=['csv'])
    
    if 'click' not in st.session_state:
        st.session_state.click = False
    
    
    def onClickFunction():
        st.session_state.click = True
       
    
    def load_lottieurl(url2: str):
        r = requests.get(url2)
        if r.status_code != 200:
            return None
        return r.json()
    lottie_url_hello = "https://lottie.host/57b82a4f-04ed-47c1-9be6-d9bdf4a4edf0/whycX7qYPw.json"
    lottie_url_download = "https://lottie.host/57b82a4f-04ed-47c1-9be6-d9bdf4a4edf0/whycX7qYPw.json"
    lottie_hello = load_lottieurl(lottie_url_hello)
    lottie_download = load_lottieurl(lottie_url_download)
    
    datos= st.empty()
    runButton= st.empty()
    progress_status=st.empty()
    progress_bar=st.empty()
    
    resp = requests.get('https://raw.githubusercontent.com/omardiosdado/emoji/main/emoji.json')
    json = resp.json()
    codes, emojis = zip(*json.items())
    emoji=pd.DataFrame({
        'Emojis': emojis,
        'Shortcodes': [f':{code}:' for code in codes],
    })
    emojis = emoji['Shortcodes']
    
    if APOLLO_CSV is not None:
    
        APOLLO_RAW = pd.read_csv(APOLLO_CSV, encoding='utf-8')
        datos.dataframe(APOLLO_RAW)
        runButton.button('Procesar datos',on_click=onClickFunction)
    
        if st.session_state.click:
            runButton.empty()
            datos.empty()
    
            with st_lottie_spinner(lottie_download, key="download", height=200, width=300):
                
                progress_status.caption(f'Iniciando... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=0
                progress_bar.progress(nbar)
                time.sleep(2)
                #GOOGLE SHEETS
                spreadsheet_key1 = '18yIcld6VZXw1MZkE0YFQVTqnpyI03p5_ZGBtWLx0xXw'
                spreadsheet_key2 = '1oF5ThuOrFfwyEJ6-40_PwqGV9UCzJwLLz1vG9wnZTWg'
                spreadsheet_key3 = '1g9_Jr0BXMqOcC5w3TKzfo7R1GJWFP-WlCQwIj2aCi2w'
    
                progress_status.caption(f'Cargando base actual... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                UPLOAD= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key3,'APOLLO_OUTPUT')
                
                progress_status.caption(f'Cargando LEADS_DB... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                LEADS_DB= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key1,'LEADS_DB')
    
                progress_status.caption(f'Cargando BLACKLIST... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                BLACKLIST= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key2,'BLACKLIST')
                
                progress_status.caption(f'Cargando BLACKLIST_WEB... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                BLACKLIST_WEB= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key2,'BL_WEB')
    
                progress_status.caption(f'Cargando CLIENTS... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                CLIENTS= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key3,'ACTIVE_CLIENTS')
    
                progress_status.caption(f'Cargando LEADS_RESP... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                LEADS_RESP= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key2,'LEADS2.0')
    
    
    
                progress_status.caption(f'Cargando datos fuente... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                NOMBRES_OK = pd.read_csv('./nombres_ok.csv', encoding='utf-8')
                NOMBRES_EXCL = pd.read_csv('./nombres_excl.csv', encoding='utf-8')
                COMPANY_EXCL = pd.read_csv('./company_excl.csv', encoding='utf-8')
                STATUS_CHECK = pd.read_csv('./status.csv', encoding='utf-8')
    
                progress_status.caption(f'Filtrando datos... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                # QUITAMOS REPETIDOS
    
                if len(UPLOAD) is not 0:
                    df0 = APOLLO_RAW[~APOLLO_RAW['Email'].isin(UPLOAD['Email'])]
                    FILTRO_REPETIDO_sheets=len(APOLLO_RAW)-len(df0)
                else:
                    df0 = APOLLO_RAW
                    FILTRO_REPETIDO_sheets=len(APOLLO_RAW)-len(df0)
                df = df0[~df0['Email'].isin(LEADS_DB['MAIL'])]
                df = df.drop(columns=[ 'Email Confidence', 'Departments', 'Contact Owner','Work Direct Phone', 'Home Phone', 'Mobile Phone', 'Corporate Phone','Other Phone', 'Stage', 'Last Contacted', 'Account Owner', 'Keywords', 'Facebook Url', 'Twitter Url','Annual Revenue', 'Total Funding', 'Latest Funding','Latest Funding Amount', 'Last Raised At', 'Email Sent', 'Email Open', 'Email Bounced', 'Replied', 'Demoed', 'Number of Retail Locations', 'Apollo Contact Id', 'Apollo Account Id'], errors='ignore')
                df.loc[:, 'DOMAIN_CHECK'] = df['Email'].str.split('@').str[1]
                FILTRO_REPETIDO=len(APOLLO_RAW)-len(df)
                #QUITAMOS VACIOS
                df1 = df.dropna(subset=['First Name', 'Company Name for Emails', 'Email','Person Linkedin Url','Website'], how='any')
                df1 = df1.reset_index(drop=True)
                FILTRO_VACIOS=len(df)-len(df1)
                #QUITAMOS EMAILS NO VERIFICADOS
                df2 = df1[df1['Email Status'] == 'Verified']
                FILTRO_EMAIL=len(df1)-len(df2)
                #QUITAMOS COUNTRY <> MEX
                df3 = df2[(df2['Country'] == 'Mexico') | (df2['Company Country'] == 'Mexico')]
                FILTRO_CONTRY=len(df2)-len(df3)
    
                progress_status.caption(f'Validando info de Apollo... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                #CHECK NOMBRE
                df3['CHECK_NAME'] = 0
                df3.loc[df3['First Name'].isin(NOMBRES_OK['NAME']), 'CHECK_NAME'] = 1
                df3.loc[df3['First Name'].isin(NOMBRES_EXCL['NAME']), 'CHECK_NAME'] = -1
                # CHECK EMAIL
                email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                def is_valid_email(email):
                    return bool(re.match(email_pattern, email))
                df3['CHECK_EMAIL'] = df3['Email'].apply(is_valid_email)
                ending_strings = COMPANY_EXCL['NAME'].tolist()
                df3['Company Name for Emails']=df3['Company Name for Emails'].str.replace(r' S\.C$', '', regex=True)
                df3['Company Name for Emails']=df3['Company Name for Emails'].str.replace(r' S\.A DE C\.V$', '', regex=True)
                df3['Company Name for Emails']=df3['Company Name for Emails'].str.replace(r' S\.A\. de C\.V$', '', regex=True)
                df3['Company Name for Emails']=df3['Company Name for Emails'].str.replace(r' SA de CV$', '', regex=True)
                df3['Company Name for Emails']=df3['Company Name for Emails'].str.replace(r' S\.A de C\.V$', '', regex=True)
                df3['CHECK_COMPANY'] = np.where(df3['Company Name for Emails'].str.endswith(tuple(ending_strings)), 0, 1)
                # MODIFICAR EMPLEADOS A NUMERICO
                df3['# Employees'] = pd.to_numeric(df3['# Employees'], errors='coerce')
    
                progress_status.caption(f'Validando info de Apollo... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                # SCORING
                # Score country
                df3['SCORE_Country']=df3['Company Country']== 'Mexico'
                df3['SCORE_Country'] = df3['SCORE_Country'].astype(int)
                df3['CHECK_EMAIL'] = df3['CHECK_EMAIL'].astype(int)
                df3['CHECK_SCORE'] = df3[['CHECK_NAME','CHECK_EMAIL', 'CHECK_COMPANY','SCORE_Country']].sum(axis=1)/4
                for index, row in CLIENTS.iterrows():
                    BL_WEB_CLIENT = BLACKLIST_WEB[BLACKLIST_WEB['CLIENTE'] == row[0]]
                    df3['BLACKLIST_'+row[0]] = df3['DOMAIN_CHECK'].isin(BL_WEB_CLIENT['CLEAN WEB'])
                    df3['BLACKLIST_'+row[0]] = df3['BLACKLIST_'+row[0]].replace({True: 0, False: 1})
                    df_client=LEADS_RESP[LEADS_RESP['CLIENT'] == row[0]]
                    df_client = df_client[df_client['STATUS'].isin(STATUS_CHECK['Status'])]
                    df_client = df_client[['EMAIL']]
                    if len(df_client)>0:
                        # Score EMPLEADOS
                        EMPLOYEES=LEADS_DB[LEADS_DB['MAIL'].isin(df_client['EMAIL'])][['EMPLOYEES']].drop_duplicates().reset_index(drop=True)
                        EMPLOYEES['EMPLOYEES'] = pd.to_numeric(EMPLOYEES['EMPLOYEES'], errors='coerce')
                        EMP_MIN=(min(EMPLOYEES.dropna()['EMPLOYEES']))
                        EMP_MAX=(max(EMPLOYEES.dropna()['EMPLOYEES']))    
                        df3['SCORE_EMP_'+row[0]] = (df3['# Employees'] >= EMP_MIN) & (df3['# Employees'] <= EMP_MAX)
                        # Score Title
                        TITLE=LEADS_DB[LEADS_DB['MAIL'].isin(df_client['EMAIL'])][['TITLE']].drop_duplicates().reset_index(drop=True)
                        df3['SCORE_TITLE_'+row[0]]=df3['Title'].isin(TITLE['TITLE'])
                        # Score seniority
                        SENIORITY=LEADS_DB[LEADS_DB['MAIL'].isin(df_client['EMAIL'])][['Seniority']].drop_duplicates().reset_index(drop=True)
                        df3['SCORE_Seniority_'+row[0]]=df3['Seniority'].isin(SENIORITY['Seniority'])
                        # Score industria
                        INDUSTRY=LEADS_DB[LEADS_DB['MAIL'].isin(df_client['EMAIL'])][['INDUSTRY']].drop_duplicates().reset_index(drop=True)
                        df3['SCORE_Industry_'+row[0]]=df3['Industry'].isin(INDUSTRY['INDUSTRY'])
                        df3['SCORE_EMP_'+row[0]] = df3['SCORE_EMP_'+row[0]].astype(int)
                        df3['SCORE_TITLE_'+row[0]] = df3['SCORE_TITLE_'+row[0]].astype(int)
                        df3['SCORE_Seniority_'+row[0]] = df3['SCORE_Seniority_'+row[0]].astype(int)
                        df3['SCORE_Industry_'+row[0]] = df3['SCORE_Industry_'+row[0]].astype(int)
                        df3['SCORE_'+row[0]] = (((df3[['SCORE_EMP_'+row[0], 'SCORE_TITLE_'+row[0],'SCORE_Seniority_'+row[0],'SCORE_Industry_'+row[0]]].sum(axis=1)/4)*df3['BLACKLIST_'+row[0]]).astype(float)*100).astype(int).astype(str)
    
                dft=df3
                clientx=''
                SCORE_FINAL=''
                dft['SCORE_FINAL']=SCORE_FINAL
                for index_client, row_client in CLIENTS.iterrows():
                    clientx = row_client['CLIENT']
                    for index_dft, row_dft in dft.iterrows():
                        score_column = 'SCORE_' + clientx
                        if score_column in row_dft and row_dft[score_column] == '100':
                            if row_dft['SCORE_FINAL']=='':
                                dft.at[index_dft, 'SCORE_FINAL'] = clientx
                            else:
                                dft.at[index_dft, 'SCORE_FINAL'] = clientx + '|' + row_dft['SCORE_FINAL']
                #################                
                clientx=''
                BL_FINAL=''
                dft['BL_FINAL']=BL_FINAL
                for index_client, row_client in CLIENTS.iterrows():
                    clientx = row_client['CLIENT']
                    for index_dft, row_dft in dft.iterrows():
                        score_column = 'BLACKLIST_' + clientx
                        if score_column in row_dft and row_dft[score_column] == 0:
                            if row_dft['BL_FINAL']=='':
                                dft.at[index_dft, 'BL_FINAL'] = clientx
                            else:
                                dft.at[index_dft, 'BL_FINAL'] = clientx + '|' + row_dft['SCORE_FINAL']
                dft['BL_FINAL'].replace('', 'NA', inplace=True)
                column = dft.pop('BL_FINAL')
                dft.insert(0, column.name, column)
                dft.pop('Email Status')
                dft.pop('Company')
                dft['Country']=dft['Country']+'_'+dft['Company Country']
                dft.pop('Company Country')
                ##############
                dft['SCORE_FINAL'].replace('', 'NA', inplace=True)
                column = dft.pop('SCORE_FINAL')
                dft.insert(0, column.name, column)
                column = dft.pop('CHECK_SCORE')
                dft.insert(0, column.name, column)
                dft['USER']=name_user
                column = dft.pop('USER')
                dft.insert(0, column.name, column)                
                progress_status.caption(f'Scrapeando linkedin... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
    
                df4=dft
                # APIFY_API_TOKEN=st.secrets['APIFY_API_TOKEN']
                # def linkedin_scraper(url_test):
                #     crawl_input = {"url": url_test}
                #     apify_client = ApifyClient(APIFY_API_TOKEN)
                #     actor_call = apify_client.actor('lordflotrox/linkedin-profile').call(run_input=crawl_input)
                #     x=[]
                #     if actor_call['status']=='SUCCEEDED':
                #         for item in apify_client.dataset(actor_call["defaultDatasetId"]).iterate_items():
                #             try:
                #                 try:
                #                     x.append(item['data']['@graph'][0]['address']['addressCountry']+'|'+item['data']['@graph'][0]['address']['addressLocality'])
                #                 except:
                #                     x.append("NA")
                #                 try:
                #                     x.append(item['data']['@graph'][0]['description'])
                #                 except:
                #                     x.append("NA")
                #                 try:
                #                     x.append(item['data']['@graph'][0]['interactionStatistic']['userInteractionCount'])
                #                 except:
                #                     x.append("NA")
                #                 z=[]
                #                 for i in item['data']['@graph'][0]['worksFor']:
                #                     y=[]
                #                     try:
                #                         y.append(i['name'])
                #                     except:
                #                         y.append("NA")
                #                     try:
                #                         y.append(i['member']['description'])
                #                     except:
                #                         y.append("NA")
                #                     try:
                #                         y.append(i['member']['startDate'])
                #                     except:
                #                         y.append("NA")
                #                     z.append(y)
                #                 x.append(z)
                #             except:
                #                 x=[]
                #     return x
                # for index, row in df4.iterrows():
                #     x=linkedin_scraper(row['Person Linkedin Url'])
                #     if len(x)>0:
                #         df4.loc[index, 'LKN_LOC']=x[0]
                #         df4.loc[index, 'LKN_DESC']=x[1]
                #         df4.loc[index, 'LKN_Follower']=x[2]
                #         n=0
                #         for i in x[3]:
                #             n=n+1
                #             df4.loc[index, ('LKN_COMP_'+ str(n))]=i[0]
                #             df4.loc[index, ('LKN_COMP_DESC_'+ str(n))]=i[1]
                #             df4.loc[index, ('LKN_COMP_START_'+ str(n))]=i[2]
                
                progress_status.caption('Guardando archivo... :yarn:')    
                nbar=5+nbar
                progress_bar.progress(nbar)
                time.sleep(2)
                
                csv_data = df4.to_csv(index=False)
               
    
                progress_status.caption(f'Formateando sheets... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
    
                df5 = df4
                gc = gspread.authorize(credentials)
    
                worksheet = gc.open_by_key(spreadsheet_key3)
                target_sheet = worksheet.worksheet('APOLLO_OUTPUT')
                df6 = pd.DataFrame(columns=UPLOAD.columns)
                
    
                for index, row in df5.iterrows():
                    df6 = pd.concat([df6, row.to_frame().T], ignore_index=True)
                    # # Check if the row from df2 is not in df
                    # if len(UPLOAD) == 0:
                    #     df6 = pd.concat([df6, row.to_frame().T], ignore_index=True)
                    # if not UPLOAD[(UPLOAD['Email'] == row['Email'])].any().any() and len(UPLOAD) != 0:
                    #     # If the row is not in df, append it to df3
                    #     # df6 = df6.append(row, ignore_index=True)
                        # df6 = pd.concat([df6, row.to_frame().T], ignore_index=True)
                df_combined = pd.concat([UPLOAD, df6], ignore_index=True)
                target_sheet.clear()
                df_combined=df_combined.astype(str)
                data_to_import = [df_combined.columns.tolist()] + df_combined.values.tolist()
                # target_sheet.insert_rows(data_to_import, 1)
    
                num_rowsx= len(data_to_import)
                num_columnsx = len(data_to_import[0])
                def get_column_label(column_index):
                    label = ""
                    while column_index > 0:
                        column_index, remainder = divmod(column_index - 1, 26)
                        label = chr(65 + remainder) + label
                    return label
                ending_column_label = get_column_label(num_columnsx)
                range_to_updatex = f'A1:{ending_column_label}{num_rowsx}'
                target_sheet.update(range_to_updatex, data_to_import)
                
                progress_status.caption(f'Cargando a sheets... {emojis[random.randint(0, len(emojis) - 1)]}')
                nbar=5+nbar
                progress_bar.progress(nbar)
                UPLOAD= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key3,'APOLLO_OUTPUT')
    
                # name_col=(UPLOAD.columns.get_loc('First Name')+1)
                # name_check_col=(UPLOAD.columns.get_loc('CHECK_NAME'))
                # Email_col=(UPLOAD.columns.get_loc('Email')+1)
                # Email_check_col=(UPLOAD.columns.get_loc('CHECK_EMAIL'))
                # Company_col=(UPLOAD.columns.get_loc('Company')+1)
                # Company_check_col=(UPLOAD.columns.get_loc('CHECK_COMPANY'))
                # Country_col=(UPLOAD.columns.get_loc('Country')+1)
                # Country_check_col=(UPLOAD.columns.get_loc('SCORE_Country'))
    
                # format_good={
                #     "backgroundColor": {
                #     "red": 0.5,
                #     "green": 0.9,
                #     "blue": 0.5,
                #     'alpha':0
                #     }
                # }
                # format_reg={
                #     "backgroundColor": {
                #     "red": 1,
                #     "green": .89,
                #     "blue": .6,
                #     'alpha':0
                #     }
                # }
                # format_bad={
                #     "backgroundColor": {
                #     "red": 0.9,
                #     "green": 0.5,
                #     "blue": 0.5,
                #     'alpha':0
                #     }
                # }
                # format_zero={
                #     "backgroundColor": {
                #     "red": 1.0,
                #     "green": 1.0,
                #     "blue": 1.0,
                #     'alpha':0
                #     }
                # }
    
                # target_sheet.format(chr(64 + name_col)+':'+chr(64 + name_col),format_zero)
                # target_sheet.format(chr(64 + Email_col)+':'+chr(64 + Email_col),format_zero)
                # target_sheet.format(chr(64 + Company_col)+':'+chr(64 + Company_col),format_zero)
                # target_sheet.format(chr(64 + Country_col)+':'+chr(64 + Country_col),format_zero)
    
                datos.dataframe(df6)
                LIMPIOS_TOT=(str(len(APOLLO_RAW)-len(df4)))
                st.session_state.click = False
                progress_bar.progress(100)
                progress_status.caption('Archivo cargado a sheets :plunger:')   
                progress_bar.empty()
                
                code = f'''
                Leads iniciales: {(str(len(APOLLO_RAW)))}
                 -> Leads repetidos en base PROSPECTOS: {FILTRO_REPETIDO_sheets}
                 -> Leads repetidos en base LEADS_DB: {FILTRO_REPETIDO}
                 -> Leads con correo no válido: {FILTRO_VACIOS}
                 -> Leads con campos importantes vacios: {FILTRO_EMAIL}
                 -> Leads con País no válido: {FILTRO_CONTRY}
                Leads NO procesados totales: {LIMPIOS_TOT}
                Procesados: {(str(len(df4)))}
                '''
                st.code(code, line_numbers=False)
                st.download_button("Download CSV", csv_data, key="download_df.csv", help="Click to download the DataFrame as CSV")
            st.balloons()
