# Define our imports
import numpy as np
import matplotlib.pyplot as plt

from pyspark.sql import DataFrame, SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from sklearn.metrics import ConfusionMatrixDisplay

# Define our constants
TARGET_DATA = "../../data/spotify_enhanced_dataset.csv"
RANDOM_SEED = 0
TARGET_FEATURES = [
    "popularity",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "duration_ms",
    "time_signature",
]


def feature_transformer(df: DataFrame) -> DataFrame:
    """Assembles feature vectors in dataframe

    Args:
        df (DataFrame): input

    Returns:
        DataFrame: feature-label dataframe
    """
    va = VectorAssembler(inputCols=TARGET_FEATURES, outputCol="features")
    va_df = va.transform(df)
    return va_df


def load_data(filename: str) -> DataFrame:
    """Load data from file

    Args:
        filename (str): filename

    Returns:
        DataFrame: pyspark dataframe
    """
    spark = SparkSession.builder.appName("hit-song-classifier-rfc").getOrCreate()
    return spark.read.csv(filename, header=True, inferSchema=True)


def split_data(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Splits data to train, val, test splits

    Args:
        df (DataFrame): full dataframe

    Returns:
        DataFrame: _description_
    """
    train, val, test = df.randomSplit([0.6, 0.2, 0.2], seed=RANDOM_SEED)
    return train, val, test


df = feature_transformer(load_data(TARGET_DATA))
train_df, val_df, test_df = split_data(df)
print(f"Train: {train_df.count()}\nTest: {test_df.count()}\nVal: {val_df.count()}")


def train_estimators(
        data: DataFrame,
        estimator_type: any,
        param_name: str,
        param_vals: list[int],
        **kwargs,
) -> list:
    """Trains estimators

    Args:
        data (DataFrame): dataframe
        estimator_type (RandomForestClassifier): estimator
        param_name (str): parameter name
        param_vals (list[int]): parameter values

    Returns:
        list: list of trained estimators
    """
    estimators = []
    for val in param_vals:
        estimator = estimator_type(**{param_name: val}, **kwargs)
        one_vs_rest = OneVsRest(classifier=estimator, **kwargs)
        fitted_models = one_vs_rest.fit(data)
        estimators.append(fitted_models)
        print(f"Training {fitted_models}")
    return estimators


reg_params = [1e-5, 1e-4, 1e-3, 1e-2, 0.1, 1]
lr_reg_params_list = train_estimators(
    train_df,
    LinearSVC,
    "regParam",
    reg_params,
    featuresCol="features",
    labelCol="class",
)


def score_model(models: list, data: DataFrame) -> list:
    """Scores models

    Args:
        estimators (list): list of estimators
        data (DataFrame): dataframe

    Returns:
        list: list of scores
    """
    evaluator = MulticlassClassificationEvaluator(
        labelCol="class", predictionCol="prediction", metricName="f1"
    )

    predicted = [model.transform(data) for model in models]
    return [evaluator.evaluate(pred) for pred in predicted]


print(f"train: {score_model(lr_reg_params_list, train_df)}")
print(f"val: {score_model(lr_reg_params_list, val_df)}")
print(f"test: {score_model(lr_reg_params_list, test_df)}")

max_iter = [1, 5, 10, 20, 50, 100]
lr_max_iter_list = train_estimators(
    train_df,
    LinearSVC,
    "maxIter",
    max_iter,
    featuresCol="features",
    labelCol="class",
)


def cross_validator(train_data: DataFrame) -> CrossValidatorModel:
    """Performs cross validation"""

    # Define the estimator
    lr = LinearSVC(featuresCol="features",
        labelCol="class",)
    one_vs_rest = OneVsRest(classifier=lr,
                            featuresCol="features",
                            labelCol="class",)

    # Define the parameter grid
    param_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, reg_params)
        .addGrid(lr.maxIter, max_iter)
        .build()
    )

    # Define the evaluator using F1 score
    evaluator = MulticlassClassificationEvaluator(
        labelCol="class", predictionCol="prediction", metricName="f1"
    )

    # Create the cross validator with 5 folds
    cv = CrossValidator(
        estimator=one_vs_rest, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3
    )
    cv_model = cv.fit(train_data)

    return cv_model


cv_model = cross_validator(train_df)

x = 10
