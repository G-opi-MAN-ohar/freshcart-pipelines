# %%writefile iris_project/transformers/evaluate_model.py
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def evaluate(df, *args, **kwargs):
    combined_df = df.copy()

    # If the 3 model blocks (DT, RF, KNN) are connected directly as upstream blocks,
    # pull their prediction columns from *args into combined_df automatically
    for extra_df in args:
        if isinstance(extra_df, pd.DataFrame):
            for col in ['predicted', 'predicted_dt', 'predicted_rf', 'predicted_knn']:
                if col in extra_df.columns and col not in combined_df.columns:
                    combined_df[col] = extra_df[col].values

    y_true = combined_df['target']

    # Support either 'predicted' or 'predicted_dt' for the Decision Tree column
    dt_col = 'predicted_dt' if 'predicted_dt' in combined_df.columns else 'predicted'
    model_columns = [
        ('Decision Tree', dt_col),
        ('Random Forest', 'predicted_rf'),
        ('KNN', 'predicted_knn'),
    ]

    metrics_data = {'metric': ['accuracy', 'precision', 'recall', 'f1_score']}

    for model_name, col_name in model_columns:
        y_pred = combined_df[col_name]

        print(f'=== {model_name} ({col_name}) Confusion Matrix ===')
        print(pd.crosstab(y_true, y_pred, rownames=['actual'], colnames=[col_name]))
        print()

        metrics_data[model_name] = [
            round(accuracy_score(y_true, y_pred), 3),
            round(precision_score(y_true, y_pred, average='macro'), 3),
            round(recall_score(y_true, y_pred, average='macro'), 3),
            round(f1_score(y_true, y_pred, average='macro'), 3),
        ]

    metrics = pd.DataFrame(metrics_data)
    print('=== Model Evaluation Summary ===')
    print(metrics.to_string(index=False))
    return metrics