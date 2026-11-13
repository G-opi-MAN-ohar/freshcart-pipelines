# %%writefile iris_project/transformers/train_random_forest.py
import joblib
from sklearn.ensemble import RandomForestClassifier

# Import Mage's decorators only if they aren't already loaded
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# The 4 measurement columns the model uses to predict the species
FEATURES = ['sepal length (cm)', 'sepal width (cm)',
            'petal length (cm)', 'petal width (cm)']


# transformer receives prepare_data's table (150 rows with 'split') as 'df'
# Reads max_depth and n_estimators from Mage's pipeline variables, then trains a random forest on the train rows
# Predicts the 30 test rows, prints accuracy, saves the model and returns the predictions
@transformer
def train_rf(df, *args, **kwargs):
    max_depth = int(kwargs.get('max_depth', 3))
    n_estimators = int(kwargs.get('n_estimators', 100))

    train_df = df[df['split'] == 'train']
    test_df = df[df['split'] == 'test'].copy()

    model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=42,
    )
    model.fit(train_df[FEATURES], train_df['target'])

    test_df['predicted_rf'] = model.predict(test_df[FEATURES])
    accuracy = (test_df['predicted_rf'] == test_df['target']).mean()
    print('Random Forest | max_depth =', max_depth, '| Test accuracy =', round(accuracy, 3))

    joblib.dump(model, 'iris_rf_model.pkl')
    return test_df


# Runs right after train_rf finishes, using its 30 predicted rows as 'output'
# Recalculates accuracy by comparing 'predicted_rf' with the true 'target'
# If accuracy is below 80%, the block fails and save_results / evaluate_model never run
@test
def test_accuracy(output, *args):
    accuracy = (output['predicted_rf'] == output['target']).mean()
    assert accuracy >= 0.8, 'Random Forest model accuracy is below 80%'