import sys

import pandas as pd
from pandas import DataFrame

from US_Visa_Approval.exception import USvisaException
from US_Visa_Approval.logger import logging
from US_Visa_Approval.utils.main_utils import read_yaml_file, write_yaml_file
from US_Visa_Approval.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from US_Visa_Approval.entity.config_entity import DataValidationConfig
from US_Visa_Approval.constants import SCHEMA_FILE_PATH


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        """
        :param data_ingestion_artifact: Output reference of data ingestion artifact stage
        :param data_validation_config: configuration for data validation
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config =read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e,sys)

    def validate_number_of_columns(self, dataframe: DataFrame) -> bool:
        """
        Method Name :   validate_number_of_columns
        Description :   This method validates the number of columns
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Is required column present: [{status}]")
            return status
        except Exception as e:
            raise USvisaException(e, sys)

    def is_column_exist(self, df: DataFrame) -> bool:
        """
        Method Name :   is_column_exist
        Description :   This method validates the existence of a numerical and categorical columns
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            if len(missing_numerical_columns)>0:
                logging.info(f"Missing numerical column: {missing_numerical_columns}")


            for column in self._schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if len(missing_categorical_columns)>0:
                logging.info(f"Missing categorical column: {missing_categorical_columns}")

            return False if len(missing_categorical_columns)>0 or len(missing_numerical_columns)>0 else True
        except Exception as e:
            raise USvisaException(e, sys) from e

    @staticmethod
    def read_data(file_path) -> DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise USvisaException(e, sys)

    def detect_dataset_drift(self, reference_df: DataFrame, current_df: DataFrame, ) -> bool:
        """
        Method Name :   detect_dataset_drift
        Description :   This method validates if drift is detected
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            reference_columns = set(reference_df.columns)
            current_columns = set(current_df.columns)

            if reference_columns != current_columns:
                logging.info("Reference and current datasets have different columns.")
                write_yaml_file(
                    file_path=self.data_validation_config.drift_report_file_path,
                    content={
                        "drift_detected": True,
                        "reason": "different columns",
                        "reference_columns": sorted(list(reference_columns)),
                        "current_columns": sorted(list(current_columns)),
                    },
                )
                return True

            drift_report = {"drift_detected": False, "drift_details": []}
            drift_detected = False

            for column in reference_df.columns:
                ref_series = reference_df[column]
                cur_series = current_df[column]

                if not pd.api.types.is_numeric_dtype(ref_series):
                    ref_dist = ref_series.value_counts(normalize=True)
                    cur_dist = cur_series.value_counts(normalize=True)
                    all_values = set(ref_dist.index).union(cur_dist.index)
                    diff = sum(abs(ref_dist.get(val, 0) - cur_dist.get(val, 0)) for val in all_values)
                    if diff > 0.25:
                        drift_detected = True
                        drift_report["drift_details"].append({
                            "column": column,
                            "type": "categorical",
                            "difference": float(diff),
                        })
                else:
                    ref_mean = float(ref_series.mean())
                    cur_mean = float(cur_series.mean())
                    ref_std = float(ref_series.std(ddof=0))
                    cur_std = float(cur_series.std(ddof=0))
                    threshold = max(ref_std, cur_std, 1e-8) * 0.5
                    if abs(ref_mean - cur_mean) > threshold:
                        drift_detected = True
                        drift_report["drift_details"].append({
                            "column": column,
                            "type": "numerical",
                            "reference_mean": ref_mean,
                            "current_mean": cur_mean,
                            "threshold": threshold,
                        })

            drift_report["drift_detected"] = drift_detected
            write_yaml_file(file_path=self.data_validation_config.drift_report_file_path, content=drift_report)
            return drift_detected
        except Exception as e:
            raise USvisaException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Method Name :   initiate_data_validation
        Description :   This method initiates the data validation component for the pipeline
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """

        try:
            validation_error_msg = ""
            logging.info("Starting data validation")
            train_df, test_df = (DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))

            status = self.validate_number_of_columns(dataframe=train_df)
            logging.info(f"All required columns present in training dataframe: {status}")
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            status = self.validate_number_of_columns(dataframe=test_df)

            logging.info(f"All required columns present in testing dataframe: {status}")
            if not status:
                validation_error_msg += f"Columns are missing in test dataframe."

            status = self.is_column_exist(df=train_df)

            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            status = self.is_column_exist(df=test_df)

            if not status:
                validation_error_msg += f"columns are missing in test dataframe."

            validation_status = len(validation_error_msg) == 0

            if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info(f"Drift detected.")
                    validation_error_msg = "Drift detected"
                else:
                    validation_error_msg = "Drift not detected"
            else:
                logging.info(f"Validation_error: {validation_error_msg}")
                

            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message=validation_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise USvisaException(e, sys) from e