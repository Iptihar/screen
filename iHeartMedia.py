#!/usr/bin/python3

# Created by Iptihar Osman
# 2021-09-25

# This Class will be used in Airflow dag to provide functionalities responsible for tasks given by interviewer

import logging
import sqlite3
import pandas as pd

from pathlib import Path
from typing import Dict
from datetime import datetime
from argparse import ArgumentParser

class iHeartMedia:
   """

   Class that will enable users to blah blah

   """

   def __init__(self):
      """
      Parameters passed to initialize the class 

      """
      self.meta_df = pd.DataFrame(columns = ['table_nm', 'row_num', 'col_num'])

      self.RUNNERS_DICT = {
         "load_db" : self._load_db,
         "question_1": self._question_1,
         "question_2": self._question_2,
         "question_3": self._question_3
      }

      self.logger = self._get_logger()


   
   def _load_db(self, file_path, db_name):
      """
      Load the csv files from the data directory into the database
      """
      conn = sqlite3.connect(db_name)

      dir = Path.cwd()
      files = dir.glob('**/*.csv')
      count = 1
      for file in files:
         table_name = f"table_{count}"
         table_df = pd.read_csv(file, error_bad_lines=False)
         self.logger.info(f'collecting meta_data from table: {table_name}')
         self.meta_df = self.meta_df.append({'table_nm' : table_name, 'row_num' : table_df.shape[0], 'col_num' : table_df.shape[1]},ignore_index = True)
         table_df.to_sql(table_name, conn, if_exists='replace')
         self.logger.info(f'table: {table_name} created successfully!')
         count += 1



   def _question_1(self, table_nm):
      """
      What's the average number of fields across all the tables you loaded?
      """
      conn = sqlite3.connect("test.db")

      self.logger.info('extracting information from meta_data table')
      avg = self.meta_df["col_num"].mean()
      data = [['avg_col', avg]]
      df = pd.DataFrame(data, columns = ['Question', 'Answer'])
      self.logger.info(f'loading answer to {table_nm} table')
      try:
         df.to_sql(table_nm, conn, if_exists='replace')
         self.logger.info(f'table: {table_nm} created successfully!')
      except Exception as e:
         self.logger.error(f'something went wrong while creating {table_nm} table')



   def _question_2(self, table_nm):
      """
      What is the word count of every value of every table?
      """
      conn = sqlite3.connect("test.db")

      total_df = pd.DataFrame(columns = ['value', 'count'])

      for table_name in self.meta_df["table_nm"]:
         df = pd.read_sql_query(f"select * from {table_name};", conn)
         for column_nm in df:
            value_counts = df[column_nm].value_counts()
            value_count_df = pd.DataFrame(value_counts)
            value_count_df = value_count_df.reset_index()
            value_count_df.columns = ['value', 'counts for count']
            total_df = total_df.append(value_count_df)

      self.logger.info(f'loading answers to {table_nm} table')
      try:
         total_df.to_sql(table_nm, conn, if_exists='replace')
         self.logger.info(f'table: {table_nm} created successfully!')
      except Exception as e:
         self.logger.error(f'something went wrong while creating {table_nm} table')



   def _question_3(self, table_nm):
      """
      What's the total number or rows for the all the tables?
      """
      conn = sqlite3.connect("test.db")

      self.logger.info('extracting information from meta_data table')
      total_rows = self.meta_df["row_num"].sum()
      data = [['total_row', total_rows]]
      df = pd.DataFrame(data, columns = ['Question', 'Answer'])
      self.logger.info(f'loading answer to {table_nm} table')
      try:
         df.to_sql(table_nm, conn, if_exists='replace')
         self.logger.info(f'table: {table_nm} created successfully!')
      except Exception as e:
         self.logger.error(f'something went wrong while creating {table_nm} table')



   def _get_logger(self) -> logging.Logger:
      """
      set logging to console and level to info
      
      Returns:
         logging {object} -- logger class object that enables console logging
      """
      logger = logging.getLogger()
      console = logging.StreamHandler()
      logger.addHandler(console)
      logger.setLevel(logging.INFO)
      return logger


   
   def _validate_path(self, file_path: str) -> str:
      """
      Validates the file is present in given path

      Arguments:
         file_path {str} -- verify file existance

      Raises:
         FileNotFoundError -- file not found exception

      Return:
         file_path {str} -- returns string path to file


      """
      if not Path(file_path).exists():
         raise FileNotFoundError("File path does not exist.")
      return  file_path



   def _add_db_args(self, parser):
      """
      add the required arguments for loading db

      Arguments:
         parser {object} -- parser object from parser class 
      """
      parser.add_argument(
        '-f',
        '--file_path',
        help='file path to csv files to load db',
        required=False,
        type=self._validate_path,
        default="/root/data/"
      )

      parser.add_argument(
        '-d',
        '--db_name',
        help='db name to load csv files',
        required=False,
        default="test.db"
      )



   def _add_question_1_args(self, parser):
      """
      add the required arguments for loading db

      Arguments:
         parser {object} -- parser object from parser class 
      """
      parser.add_argument(
        '-t',
        '--table_name',
        help='table name to load the answer of question 1',
        required=False,
        default="question_1"
      )



   def _add_question_2_args(self, parser):
      """
      add the required arguments for loading db

      Arguments:
         parser {object} -- parser object from parser class 
      """
      parser.add_argument(
        '-t',
        '--table_name',
        help='table name to load the answer of question 2',
        required=False,
        default="question_2"
      )



   def _add_question_3_args(self, parser):
      """
      add the required arguments for loading db

      Arguments:
         parser {object} -- parser object from parser class 
      """
      parser.add_argument(
        '-t',
        '--table_name',
        help='table name to load the answer of question 2',
        required=False,
        default="question_2"
      )



   def build_parser(self) -> ArgumentParser:
      """
      pulls in passed arguments and creates parser object to be used

      Returns:
         parser {object} -- parser object from argparser class
      """
      parser = ArgumentParser(description='This script runs Airflow steps')
      subparsers = parser.add_subparsers(help="sub-command help")

      db_parser = subparsers.add_parser("load_db", help="load the database using csv file")
      db_parser.set_defaults(runner="load_db")
      self._add_db_args(db_parser)

      q1_parser = subparsers.add_parser("question_1", help="The table to load the answer of question 1")
      q1_parser.set_defaults(runner="question_1")
      self._add_question_1_args(q1_parser)

      q2_parser = subparsers.add_parser("question_2", help="The table to load the answer of question 2")
      q2_parser.set_defaults(runner="question_2")
      self._add_question_2_args(q2_parser)

      q3_parser = subparsers.add_parser("question_3", help="The table to load the answer of question 3")
      q3_parser.set_defaults(runner="question_3")
      self._add_question_3_args(q3_parser)

      return parser




   def run(self, **kwargs: Dict[str, str]):
       """
       generic runner function to run specified steps in Airflow

       Notes:

       **Required for load_db**
       - ``table`` `(str)` - The table to be loaded from csv files

       **Required for retrive**
       - ``table`` `(str)` - The table to load the answer of question 1

       **Required for ctm**
       - ``table`` `(str)` - The table to load the answer of question 2

       **Optional**
       - ``table`` `(str)` - The table to load the answer of question 3

       """
       runner = kwargs.pop("runner", None)

       if runner:
          self.RUNNERS_DICT[runner](**kwargs)
       else:
          runner_list = list(self.RUNNERS_DICT.keys())
          runner_str = ", ".join(runner_list)
          msg = f"Please use one of the type specified in this list: {runner_str}"
          raise ValueError(msg)



if __name__ == '__main__':
    tasks = iHeartMedia()
    parser = tasks.build_parser()
    job_args = parser.parse_args()
    kwargs = vars(job_args)
    tasks.run(**kwargs)