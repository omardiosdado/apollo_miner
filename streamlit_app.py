import pandas as pd
import numpy as np
import gspread
import re
import streamlit as st
import streamlit_authenticator as stauth
import requests
from streamlit_lottie import st_lottie_spinner
import time
import random
from google.oauth2.service_account import Credentials
import yaml
from yaml.loader import SafeLoader
from google.cloud import bigquery
from datetime import datetime, timedelta
from pandas import to_datetime

favicon = 'https://matrioshka.com.mx/wp-content/uploads/2020/01/cropped-favicon-32x32.png'
st.set_page_config(
    page_title="Matrioshka",
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

# st.image("https://i.imgur.com/XQ0ePg2.png", use_column_width='auto') test
st.title(':nesting_dolls: Procesador de Leads')
st.caption(':turtle: V2.02 by Polímata.AI')

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
# name, authentication_status, username = authenticator.login(fields=['username', 'password'], location='sidebar')

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
    
    #Sheets
    skey = st.secrets["gcp_service_account"]
    credentials = Credentials.from_service_account_info(
        skey,
        scopes = ["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(credentials)
    
    #BigQuery
    skey2 = st.secrets["gcp_service_account2"]
    credentials2 = Credentials.from_service_account_info(
        skey2,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client2 = bigquery.Client(credentials=credentials2, project=credentials2.project_id)


    def load_data(url, sheet_name):
        sh = client.open_by_url(url)
        df = pd.DataFrame(sh.worksheet(sheet_name).get_all_records())
        return df
    
    runButton2= col2.empty()
    uploaded_file = col2.empty()
    APOLLO_CSV = uploaded_file.file_uploader('Carga el .CSV de Apollo:', type=['csv'])
    csv_data = pd.DataFrame().to_csv(index=False)
    
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
    #lottie_url_download = "https://lottie.host/57b82a4f-04ed-47c1-9be6-d9bdf4a4edf0/whycX7qYPw.json"
    lottie_url_download ="https://lottie.host/f3a3d53c-5e90-4d8b-b4e0-acdd22003971/ARjLKWOmyX.json"

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
        APOLLO_RAW = APOLLO_RAW.drop(columns=[ 'Email Confidence', 'Contact Owner', 'Stage', 
                        'Last Contacted', 'Account Owner', 'Email Sent', 
                        'Email Open', 'Email Bounced', 'Replied', 'Demoed', 
                        'Apollo Contact Id', 'Apollo Account Id','Corporate Phone',
                        'Departments','Home Phone','Last Raised At',
                        'Mobile Phone','Number of Retail Locations','Other Phone',
                        'Work Direct Phone', 'Company'], errors='ignore')
        APOLLO_RAW = APOLLO_RAW.rename(columns={'# Employees': 'Employees',
                                                'Annual Revenue':'Annual_Revenue',
                                                'Company Address':'Company_Address',
                                                'Company City':'Company_City',
                                                'Company Linkedin Url':'Company_Linkedin_Url',
                                                'Company Name for Emails':'Company',
                                                'Company Phone':'Company_Phone',
                                                'Company State':'Company_State',
                                                'Company Country':'Company_Country',
                                                'Facebook Url':'Facebook_Url',
                                                'First Name':'First_Name',
                                                'First Phone':'First_Phone',
                                                'Last Name': 'Last_Name',
                                                'Latest Funding':'Latest_Funding',
                                                'Latest Funding Amount':'Latest_Funding_Amount',
                                                'Person Linkedin Url':'Person_Linkedin_Url',
                                                'SEO Description':'SEO_Description',
                                                'Total Funding':'Total_Funding',
                                                'Twitter Url':'Twitter_Url'})
        columns_to_keep = [
        'Lists','First_Name', 'Last_Name', 'Title', 'Company', 'Email', 'Email Status',
        'Seniority', 'First_Phone', 'Employees', 'Industry', 'Keywords',
        'Person_Linkedin_Url', 'Website', 'Company_Linkedin_Url',
        'Facebook_Url', 'Twitter_Url', 'City', 'State', 'Country',
        'Company_Address', 'Company_City', 'Company_State', 'Company_Country',
        'Company_Phone', 'SEO_Description', 'Technologies', 'Annual_Revenue',
        'Total_Funding', 'Latest_Funding', 'Latest_Funding_Amount'
        ]

        # Create a new DataFrame with only the specified columns
        APOLLO_RAW = APOLLO_RAW.filter(items=columns_to_keep)
        
        datos.dataframe(APOLLO_RAW)
        COUNT_LEADS_CARGA = len(APOLLO_RAW)
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
                spreadsheet_key1 = st.secrets['spreadsheet_key1']
                spreadsheet_key2 = st.secrets['spreadsheet_key2']
                spreadsheet_key3 = st.secrets['spreadsheet_key3']
                spreadsheet_key4 = st.secrets['spreadsheet_key4']


                gc = gspread.authorize(credentials)
                worksheet = gc.open_by_key(spreadsheet_key4).worksheet('CHECK')  # Access the first worksheet
                # cell_value = None
                # cell_value = (worksheet.acell('A1').value)
                cell_value = worksheet.acell('A1').value
                
                # if cell_value is None:
                if cell_value is None or cell_value == "None":
                    print("Cell A1 is empty or has a value that evaluates to False")
                    # worksheet.update('A1', name_user)
                    worksheet.update('A1', [[name_user]])
                    progress_status.caption(f'Cargando base actual... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)

                    QUERY = ("SELECT * FROM `matrioshka-404701.matrioshka_leads.master_leads` WHERE Status = 'AP';")
                    UPLOAD = client2.query(QUERY).to_dataframe()
                    progress_status.caption(f'Cargando LEADS_DB... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    QUERY = ("SELECT * FROM `matrioshka-404701.matrioshka_leads.master_leads` WHERE Status = 'LDB';")
                    LEADS_DB= client2.query(QUERY).to_dataframe()

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
                    ###################
                    ############
                    #################
                    #####################
                                        ###################
                    ############
                    #################
                    #####################                    ###################
                    ############
                    #################
                    #####################                    ###################
                    ############
                    #################
                    #####################
                    CLIENTS= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key3,'API')
                            ###################
                    ############
                    #################
                    #####################                    ###################
                    ############
                    #################
                    #####################                    ###################
                    ############
                    #################
                    #####################
                    progress_status.caption(f'Cargando LEADS_RESP... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    LEADS_RESP= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key2,'LEADS2.0')
        
        
        
                    progress_status.caption(f'Cargando datos fuente... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    QUERY = ('SELECT * FROM `matrioshka-404701.matrioshka_leads.nombres_ok`')
                    NOMBRES_OK= client2.query(QUERY).to_dataframe()
                    QUERY = ('SELECT * FROM `matrioshka-404701.matrioshka_leads.nombres_excl`')
                    NOMBRES_EXCL= client2.query(QUERY).to_dataframe()
                    QUERY = ('SELECT * FROM `matrioshka-404701.matrioshka_leads.company_excl`')
                    COMPANY_EXCL= client2.query(QUERY).to_dataframe()
                    QUERY = ('SELECT * FROM `matrioshka-404701.matrioshka_leads.status`')
                    STATUS_CHECK= client2.query(QUERY).to_dataframe()
                    MAIL_STATUS = client2.query("SELECT * FROM `matrioshka-404701.matrioshka_leads.MAIL_STATUS`;").to_dataframe()
                    MAIL_STATUS['verified_on'] = to_datetime(MAIL_STATUS['verified_on'])
                    four_months_ago = datetime.now() - timedelta(days=120)
                    MAIL_STATUS['more_than_4_months'] = MAIL_STATUS['verified_on'] < four_months_ago
                    MAIL_STATUS['verified_on'] = MAIL_STATUS['verified_on'].dt.strftime('%Y-%m-%d')
                    UNSAFE = MAIL_STATUS[MAIL_STATUS['safe_to_send'] == 'no']
                    

        
                    progress_status.caption(f'Filtrando datos... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    # QUITAMOS REPETIDOS
        
                    df = APOLLO_RAW
                    df.loc[:, 'DOMAIN_CHECK'] = df['Email'].str.split('@').str[1]
                    FILTRO_REPETIDO=0
                    # FILTRO_REPETIDO_sheets=0
                    

                    
                    #QUITAMOS VACIOS
                    df1 = df.dropna(subset=['First_Name', 'Company', 'Email','Person_Linkedin_Url','Website'], how='any')
                    df1 = df1.reset_index(drop=True)
                    FILTRO_VACIOS=len(df)-len(df1)
                    #QUITAMOS EMAILS NO VERIFICADOS
                    df2 = df1[~df1['Email'].isin(UNSAFE['MAIL'])]
                    personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']  # add more domains as needed
                    gov_domains_mex = ['gob.mx', '.gov.mx']  # add more Mexico government domains as needed
                    gov_domains_usa = ['gov', '.gov', 'usa.gov']  # add more USA government domains as needed
                    df2 = df2[~df2['DOMAIN_CHECK'].str.contains('|'.join(gov_domains_mex + gov_domains_usa + personal_domains), na=False)]
                    # df2 = df1[df1['Email Status'] == 'Verified']
                    # df2 = df1
                    FILTRO_EMAIL=len(df1)-len(df2)
                    #QUITAMOS COUNTRY <> MEX
                    df3=df2
                    #df3 = df2[(df2['Country'] == 'Mexico') | (df2['Company Country'] == 'Mexico')]
                    FILTRO_CONTRY=len(df2)-len(df3)
                    progress_status.caption(f'Validando info de Apollo... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    #CHECK NOMBRE
                    df3['CHECK_NAME'] = 0
                    df3.loc[df3['First_Name'].isin(NOMBRES_OK['NAME']), 'CHECK_NAME'] = 1
                    df3.loc[df3['First_Name'].isin(NOMBRES_EXCL['NAME']), 'CHECK_NAME'] = -1
                    # CHECK EMAIL
                    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                    def is_valid_email(email):
                        return bool(re.match(email_pattern, email))
                    df3['CHECK_EMAIL'] = df3['Email'].apply(is_valid_email)
                    ending_strings = COMPANY_EXCL['NAME'].tolist()
                    df3['Company']=df3['Company'].str.replace(r' S\.C$', '', regex=True)
                    df3['Company']=df3['Company'].str.replace(r' S\.A DE C\.V$', '', regex=True)
                    df3['Company']=df3['Company'].str.replace(r' S\.A\. de C\.V$', '', regex=True)
                    df3['Company']=df3['Company'].str.replace(r' SA de CV$', '', regex=True)
                    df3['Company']=df3['Company'].str.replace(r' S\.A de C\.V$', '', regex=True)
                    df3['CHECK_COMPANY'] = np.where(df3['Company'].str.endswith(tuple(ending_strings)), 0, 1)
                    # MODIFICAR EMPLEADOS A NUMERICO
                    df3['Employees'] = pd.to_numeric(df3['Employees'], errors='coerce')
        
                    progress_status.caption(f'Validando info de Apollo... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    # SCORING
                    # Score country
                    df3['SCORE_Country']=df3['Company_Country']== 'Mexico'
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
                            EMPLOYEES=LEADS_DB[LEADS_DB['Email'].isin(df_client['EMAIL'])][['Employees']].drop_duplicates().reset_index(drop=True)
                            EMPLOYEES['Employees'] = pd.to_numeric(EMPLOYEES['Employees'], errors='coerce')
                            if not EMPLOYEES.dropna().empty:
                                EMP_MIN = min(EMPLOYEES.dropna()['Employees'])
                                EMP_MAX = max(EMPLOYEES.dropna()['Employees'])
                            else:
                                # Handle the case when EMPLOYEES is empty
                                EMP_MIN = None  # or some default value
                                EMP_MAX = None  # or some default value
    
                            
                            # EMP_MIN=(min(EMPLOYEES.dropna()['EMPLOYEES']))
                            # EMP_MAX=(max(EMPLOYEES.dropna()['EMPLOYEES']))    
                            df3['SCORE_EMP_'+row[0]] = (df3['Employees'] >= EMP_MIN) & (df3['Employees'] <= EMP_MAX)
                            # Score Title
                            TITLE=LEADS_DB[LEADS_DB['Email'].isin(df_client['EMAIL'])][['Title']].drop_duplicates().reset_index(drop=True)
                            df3['SCORE_TITLE_'+row[0]]=df3['Title'].isin(TITLE['Title'])
                            # Score seniority
                            SENIORITY=LEADS_DB[LEADS_DB['Email'].isin(df_client['EMAIL'])][['Seniority']].drop_duplicates().reset_index(drop=True)
                            df3['SCORE_Seniority_'+row[0]]=df3['Seniority'].isin(SENIORITY['Seniority'])
                            # Score industria
                            INDUSTRY=LEADS_DB[LEADS_DB['Email'].isin(df_client['EMAIL'])][['Industry']].drop_duplicates().reset_index(drop=True)
                            df3['SCORE_Industry_'+row[0]]=df3['Industry'].isin(INDUSTRY['Industry'])
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
                    dft['Country_Country']=dft['Country']+'_'+dft['Company_Country']
                    dft['Status']='AP'
                    ##############
                    dft['SCORE_FINAL'].replace('', 'NA', inplace=True)
                    dft['USER']=name_user
                    dft['USER'] = dft['USER'].astype(str)
                    progress_status.caption(f'Scrapeando linkedin... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    for index, row in CLIENTS.iterrows():
                        clientx = row['CLIENT']
                        dft.drop(columns=[ 
                            'BLACKLIST_' + clientx, 
                            'SCORE_' + clientx, 
                            'SCORE_EMP_' + clientx,
                            'SCORE_Industry_' + clientx, 
                            'SCORE_Seniority_' + clientx, 
                            'SCORE_TITLE_' + clientx
                        ], inplace=True, errors='ignore')      
                    df4=dft
                    
                    progress_status.caption('Guardando archivo... :yarn:')    
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    time.sleep(2)
                    
                    csv_data = df4.to_csv(index=False)
                   
                    progress_status.caption(f'Formateando... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    df5 = df4

                    gc = gspread.authorize(credentials)
                    worksheet = gc.open_by_key(spreadsheet_key4)
                    target_sheet = worksheet.worksheet('CARGA_LEADS')
                    CARGA_LEADS = pd.DataFrame(target_sheet.get_all_records())
                    
                    df6 = pd.DataFrame(columns=CARGA_LEADS.columns)
                    
                    for index, row in df5.iterrows():
                        df6 = pd.concat([df6, row.to_frame().T], ignore_index=True)
                    df_combined = df6
                    
                    df_combined['SNIPPET_1']='NA'
                    df_combined['SNIPPET_2']='NA'
                    df_combined['SNIPPET_3']='NA'
                    df_combined['SNIPPET_4']='NA'
                    df_combined['SNIPPET_5']='NA'
                    df_combined['OBS']='NA'
                    df_combined['STS']='🟡'
                    df_combined['CLIENTE']='0_ESPERANDO..'
                    # for index, row in df5.iterrows():
                    #     df6 = pd.concat([df6, row.to_frame().T], ignore_index=True)

                    # #  NUEVO PROCESO /////////////////////////////////////
                    # df_combined = df6
                    # df_combined.insert(loc=0, column='SNIPPET_1', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='SNIPPET_2', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='SNIPPET_3', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='SNIPPET_4', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='SNIPPET_5', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='OBS', value=['NA'] * len(df_combined))
                    # df_combined.insert(loc=0, column='STS', value=['🟡'] * len(df_combined))
                    # df_combined.insert(loc=0, column='CLIENTE', value=['0_ESPERANDO..'] * len(df_combined))


                    df_combined=df_combined.astype(str)
                    # data_to_import = df_combined.values.tolist()
                    # data_to_add = data_to_import
                    # total_rows = len(target_sheet.get_all_values())
                    # next_row = total_rows + 1
                    # needed_rows = next_row + len(data_to_add) - 1
                    # if needed_rows > target_sheet.row_count:
                    #     target_sheet.add_rows(needed_rows - target_sheet.row_count)
                    # range_to_write = f'A{next_row}:AT{next_row + len(data_to_add) - 1}'
                    # target_sheet.update(range_to_write, data_to_add)
                    #  NUEVO PROCESO /////////////////////////////////////
                    
                    progress_status.caption(f'Cargando base... {emojis[random.randint(0, len(emojis) - 1)]}')
                    nbar=5+nbar
                    progress_bar.progress(nbar)
                    # UPLOAD= load_data('https://docs.google.com/spreadsheets/d/'+spreadsheet_key3,'APOLLO_OUTPUT')
                    table_id = 'matrioshka-404701.matrioshka_leads.master_leads'

                    job_config = bigquery.LoadJobConfig()
                    job_config.schema_update_options = [
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                    ]
                    df4['USER'] = df4['USER'].astype(str)
                    job = client2.load_table_from_dataframe(df4, table_id, job_config=job_config)
                    job.result()
               
                    datos.dataframe(df6)
                    LIMPIOS_TOT=(str(COUNT_LEADS_CARGA-len(df6)))
                    FILTRO_REPETIDO_sheets=(str(len(df6)-len(df4)))



                    gc = gspread.authorize(credentials)
                    worksheet = gc.open_by_key(spreadsheet_key4)
                    target_sheet = worksheet.worksheet('CARGA_LEADS')
                    SHEET_ID = target_sheet.id  # Get the sheet ID
                    requests = [{
                        "clearBasicFilter": {
                            "sheetId": SHEET_ID
                        }
                    }]

                    worksheet.batch_update({'requests': requests})
                    last_row = len(target_sheet.get_all_values())
                    last_column = len(target_sheet.row_values(1))
                    data_range = f'A2:{chr(64 + last_column)}{last_row}'
                    target_sheet.sort((2, 'des'), range=data_range)
                    unsafe_vect=UNSAFE[['MAIL']]
                    CARGA_LEADS = pd.DataFrame(target_sheet.get_all_records())
                    duplicates = CARGA_LEADS.duplicated(keep='first')
                    CARGA_LEADS.loc[duplicates, 'STS'] = '🟢'
                    for index, row in CARGA_LEADS.iterrows():
                        if row['Email'] in unsafe_vect['MAIL'].values or row['CLIENTE']== '1_BORRAR':
                            CARGA_LEADS.at[index, 'STS'] = '🟢'
                            CARGA_LEADS.at[index, 'OBS'] = 'UNSAFE'
                    update_data = CARGA_LEADS[['STS', 'OBS']].values.tolist()
                    range_start = 2  # Assuming you want to start updating from the second row
                    range_end = range_start + len(update_data) - 1
                    # Update the entire range in one go
                    target_sheet.update(f'B{range_start}:C{range_end}', update_data)
                    #mod abajo
                    last_row = len(target_sheet.get_all_values())
                    last_column = len(target_sheet.row_values(1))
                    data_range = f'A2:{chr(64 + last_column)}{last_row}'
                    target_sheet.sort((2, 'des'), range=data_range)                    
                    ## mod arriba
                    last_green_index = CARGA_LEADS[CARGA_LEADS['STS'] == '🟢'].last_valid_index()
                    #mod abajo
                    CARGA_LEADS = pd.DataFrame(target_sheet.get_all_records())
                    #mod arriba
                    # if last_green_index is not None:
                    #     if last_green_index >= 1:
                    #         target_sheet.delete_rows(2, int(last_green_index) + 2)
                    #     else:
                    #         target_sheet.delete_rows(2)
                    last_row = len(target_sheet.get_all_values())
                    last_column = len(target_sheet.row_values(1))
                    data_range = f'A2:{chr(64 + last_column)}{last_row}'
                    target_sheet.sort((1, 'des'), range=data_range)
                    QUERY = ("SELECT KEY2 FROM `matrioshka-404701.matrioshka_leads.master_camp_leads`")
                    email_vect= client2.query(QUERY).to_dataframe()
                    email_vect= email_vect.drop_duplicates()
                    # email_vect = pd.concat([blacklist4, email_vect])
                    email_vect = email_vect.reset_index(drop=True)
                    CARGA_LEADS = pd.DataFrame(target_sheet.get_all_records())
                    CARGA_LEADS.insert(loc=0, column='KEY', value=['NA'] * len(CARGA_LEADS))
                    for index, row in CARGA_LEADS.iterrows():
                        if row['STS'] == '🔴' and row['OBS'] == 'REPETIDO' and row['KEY'] not in email_vect['KEY2'].values:
                            CARGA_LEADS.at[index, 'STS'] = '🟡'
                            CARGA_LEADS.at[index, 'OBS'] = 'CAMBIADO'
                        if '0_ESPERANDO..' == row['CLIENTE']:
                            continue
                        else:
                            CARGA_LEADS.at[index, 'KEY'] = row['CLIENTE'].split("_")[0] + '_' + row['Email']

                    for index, row in CARGA_LEADS.iterrows():
                        if '0_ESPERANDO..' == row['CLIENTE']:
                            continue
                        if row['KEY'] in email_vect['KEY2'].values:
                            CARGA_LEADS.at[index, 'STS'] = '🔴'
                            CARGA_LEADS.at[index, 'OBS'] = 'REPETIDO'
                    CARGA_LEADS.drop(columns=['KEY'], inplace=True)
                    update_data = CARGA_LEADS[['STS', 'OBS']].values.tolist()
                    range_start = 2  # Assuming you want to start updating from the second row
                    range_end = range_start + len(update_data) - 1
                    # Update the entire range in one go
                    target_sheet.update(f'B{range_start}:C{range_end}', update_data)
                    data_to_import = df_combined.values.tolist()
                    data_to_add = data_to_import
                    total_rows = len(target_sheet.get_all_values())
                    next_row = total_rows + 1
                    needed_rows = next_row + len(data_to_add) - 1
                    if needed_rows > target_sheet.row_count:
                        target_sheet.add_rows(needed_rows - target_sheet.row_count)
                    range_to_write = f'A{next_row}:BB{next_row + len(data_to_add) - 1}'
                    target_sheet.update(range_to_write, data_to_add)


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
                    Procesados: {(str(len(df6)))}
                    '''
                    st.code(code, line_numbers=False)
                    worksheet = gc.open_by_key(spreadsheet_key4).worksheet('CHECK')  # Access the first worksheet
                    worksheet.update(range_name='A1', values=[[""]])  # using named arguments
                # if cell_value:
                else:
                    # st.error(f'Archivo en uso por {name}'.format(cell_value))
                    st.error(f'Archivo en uso por {cell_value}')
    
                st.download_button("Download CSV", csv_data, key="download_df.csv", help="Click to download the DataFrame as CSV")
            st.balloons()
