
import ast
import sqlite3
import numpy as np
import pandas as pd
import datetime as dt
import unittest
import logging

# Set up logging
logger = logging.getLogger(__name__)
format = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(format)
file_handler = logging.FileHandler('pipeline.log')
file_handler.setFormatter(format)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

def retrieve_tables(db):
    con = sqlite3.connect(db)
    _ = con.cursor()

    logger.info("Retrieving tables from %s...", db)

    try:
        return pd.read_sql_query("SELECT * FROM cademycode_students", con), \
            pd.read_sql_query("SELECT * FROM cademycode_courses", con), \
            pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    
    except Exception as e:
        logger.error("Error retrieving tables from %s: %s", db, e)
    finally:
        con.close()

def manage_students_df(students_df: pd.DataFrame):

    logger.info("Managing students table...")
    
    # Convert date of birth to datetime
    students_df['dob'] = pd.to_datetime(students_df['dob'])

    # Calculate age in years
    current_date = dt.datetime.now()
    students_df['age'] = ((current_date - pd.to_datetime(students_df['dob'])).dt.days // 365.25).astype(int)

    # Calculate age group by decade
    students_df['age_group'] = (students_df['age'] // 10 * 10).astype(int)

    # Rename columns
    students_df.rename(columns={'dob': 'date_of_birth'}, inplace=True)

    # Evaluate contact_info as a dictionary
    students_df['contact_info'] = students_df["contact_info"].apply(lambda x: ast.literal_eval(x))

    # Explode contact_info
    explode_contact = pd.json_normalize(students_df['contact_info'])
    students_df = pd.concat([students_df.drop('contact_info', axis=1), explode_contact], axis=1)

    # Separate lines of mailing address
    students_df['address_line_1'] = students_df['mailing_address'].str.split(',').str[0].str.strip()
    students_df['city'] = students_df['mailing_address'].str.split(',').str[1].str.strip()
    students_df["state"] = students_df['mailing_address'].str.split(',').str[2].str.strip()
    students_df["zip_code"] = students_df['mailing_address'].str.split(',').str[3].str.strip()
    students_df.drop(columns="mailing_address", inplace=True)

    # Give null values a value of 0
    students_df = students_df.fillna(0)

    # Change datatypes from string to float
    students_df['job_id'] = students_df['job_id'].astype(float)
    students_df["num_course_taken"] = students_df["num_course_taken"].astype(float)
    students_df["current_career_path_id"] = students_df["current_career_path_id"].astype(float)

    # Change datatypes from float to int
    students_df['job_id'] = students_df['job_id'].astype(int)
    students_df["num_course_taken"] = students_df["num_course_taken"].astype(int)
    students_df["current_career_path_id"] = students_df["current_career_path_id"].astype(int)

    # Convert to float
    students_df['time_spent_hrs'] = students_df['time_spent_hrs'].astype(float)

    # Convert to timedelta
    students_df['time_spent'] = pd.to_timedelta(students_df['time_spent_hrs'], unit='h')

    # Drop time_spent_hrs
    students_df.drop(columns="time_spent_hrs", inplace=True)

    # Split name
    students_df['first_name'] = students_df['name'].str.split(' ').str[0]
    students_df['last_name'] = students_df['name'].str.split(' ').str[1]
    students_df.drop(columns="name", inplace=True)

    return students_df

def manage_courses_df(courses_df: pd.DataFrame):

    logger.info("Managing courses table...")

    # Define null placeholder
    na = {
    'career_path_id': 0,
    'career_path_name': 'not applicable',
    'hours_to_complete': 0
    }

    # Add null placeholder
    courses_df.loc[len(courses_df)] = na

    return courses_df

def join_dfs(students_df: pd.DataFrame, courses_df: pd.DataFrame, student_jobs_df: pd.DataFrame):

    logger.info("Joining tables...")
    final_df = students_df.merge(courses_df, left_on='current_career_path_id', right_on='career_path_id', how='left')
    return final_df.merge(student_jobs_df, on='job_id', how='left')

def main(db):
    logger.info("Running pipeline...")
    students_df, courses_df, student_jobs_df = retrieve_tables(db)
    students_df = manage_students_df(students_df)
    courses_df = manage_courses_df(courses_df)
    final_df = join_dfs(students_df, courses_df, student_jobs_df)
    return final_df

"""
#################################################################### UNIT TESTS ####################################################################
"""

class TestRetrieveTables(unittest.TestCase):
    def test_retrieve_tables(self):
        db = './data/cademycode.db'
        students_df, courses_df, student_jobs_df = retrieve_tables(db)
        self.assertIsInstance(students_df, pd.DataFrame)
        self.assertIsInstance(courses_df, pd.DataFrame)
        self.assertIsInstance(student_jobs_df, pd.DataFrame)

class TestManageStudentsDF(unittest.TestCase):

    def test_manage_students_df(self):
        mock_students_df = pd.DataFrame({
            'uuid': [1, 2, 3],
            'name': ['Jane Doe', 'John Doe', 'Jane Doe'],
            'sex': ['F', 'M', 'F'],
            'dob': ['1990-01-01', '1990-10-31', '1996-02-10'],
            'contact_info': ['{"mailing_address": "123 Main St, Anytown, New York, 12345", "email": "ZUu3F@example.com"}',
                              '{"mailing_address": "456 Main St, Sometown, Nevada, 12345", "email": "ZUu3F@example.com"}',
                                '{"mailing_address": "789 Main St, Whereville, California, 12345", "email": "ZUu3F@example.com"}'],
            'job_id': [1.0, 2.0, None],
            'current_career_path_id': [1.0, 2.0, None],
            'num_course_taken': [1.0, 2.0, None],
            'time_spent_hrs': [1.0, 2.0, None],
        })

        expected_result = pd.DataFrame({
            'uuid': [1, 2, 3],
            'sex': ['F', 'M', 'F'],
            'date_of_birth': [dt.date(1990, 1, 1), dt.date(1990, 10, 31), dt.date(1996, 2, 10)],
            'job_id': [1, 2, 0],
            'current_career_path_id': [1, 2, 0],
            'num_course_taken': [1, 2, 0],
            'age': [34, 33, 28],
            'age_group': [30, 30, 20],
            'email': ['ZUu3F@example.com', 'ZUu3F@example.com', 'ZUu3F@example.com'],
            'address_line_1': ['123 Main St', '456 Main St', '789 Main St'],
            'city': ['Anytown', 'Sometown', 'Whereville'],
            'state': ['New York', 'Nevada', 'California'],
            'zip_code': ['12345', '12345', '12345'],
            'time_spent': [dt.timedelta(hours=1), dt.timedelta(hours=2), dt.timedelta(hours=0)],
            'first_name': ['Jane', 'John', 'Jane'],
            'last_name': ['Doe', 'Doe', 'Doe']
        })

        expected_result['date_of_birth'] = expected_result['date_of_birth'].astype('datetime64[ns]')

        result = manage_students_df(mock_students_df)

        pd.testing.assert_frame_equal(result, expected_result)

    class TestManageCoursesDf(unittest.TestCase):

        def test_add_null_placeholder(self):
            
            # Test that the null placeholder is added
            df = pd.DataFrame({
                'career_path_id': [1, 2, 3],
                'career_path_name': ['A', 'B', 'C'],
                'hours_to_complete': [10, 20, 30]
            })
            expected_df = pd.DataFrame({
                'career_path_id': [1, 2, 3, 0],
                'career_path_name': ['A', 'B', 'C', 'not applicable'],
                'hours_to_complete': [10, 20, 30, 0]
            })
            result_df = manage_courses_df(df)
            self.assertTrue(result_df.equals(expected_df))

        def test_null_placeholder_is_last_row(self):

            # Test that the null placeholder is added 
            # as the last row
            df = pd.DataFrame({
                'career_path_id': [1, 2, 3],
                'career_path_name': ['A', 'B', 'C'],
                'hours_to_complete': [10, 20, 30]
            })
            result_df = manage_courses_df(df)
            self.assertEqual(result_df.iloc[-1].to_dict(), {
                'career_path_id': 0,
                'career_path_name': 'not applicable',
                'hours_to_complete': 0
            })

if __name__ == "__main__":
    unittest.main()