import streamlit as st
import pandas
import sql_metadata
from sql_metadata.compat import get_query_tables
from snowflake.snowpark import Session
from snowflake.snowpark import functions
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import udf

def main():
    with st.container():
        left,right=st.columns([1.5,1.5])
        with left:
            user=st.text_input('Enter snowflake user id')
        with right:
            pswd=st.text_input('Enter snowflake password',type='password')

    if "button_clicked" not in st.session_state:
        st.session_state.button_clicked = False

    def click_first_button():
        if "button_clicked" not in st.session_state:
            st.session_state.button_clicked = False

    if user and pswd:
        if st.button('login',on_click=click_first_button) or st.session_state.button_clicked:
            st.session_state.button_clicked = True
            connection_parameters = {'account': 'qy12204.ap-southeast-1', 'user': user, 'password': pswd,
                                     'role': 'ACCOUNTADMIN', 'warehouse': 'compute_wh', 'database': 'test_db',
                                     'schema': 'public'}

            test_session = Session.builder.configs(connection_parameters).create()

            if test_session:
                st.success('Successfully connected to snowflake')
                with st.container():
                    left, right = st.columns([1.5, 1.5])
                    with left:
                        first = st.text_input('Enter first name')
                    with right:
                        last = st.text_input('Enter last name')

                if first and last:
                    if st.button('Execute'):
                        @udf(name='fullname', input_types=[StringType(), StringType()], return_type=StringType(),session=test_session,
                             is_permanent=False,
                             replace=True)
                        def full_name_direct(first, last):
                            full = first + " " + last
                            return full

                        if test_session:
                            print('Successful')
                            output = test_session.sql("select fullname('{}','{}') as Full_name".format(first, last))
                            # output=test_session.sql("select query_parser('select * from sales') as table_names")
                            output = output.to_pandas()
                            st.dataframe(output)
                            test_session.close()
                            # test_session.close()
                        else:
                            print('Failed')
                            sys.exit(0)

main()
