# %%writefile iris_project/transformers/train_knn_classifier.py
import joblib
from sklearn.neighbors import KNeighborsClassifier

# Import Mage's decorators only if they aren't already loaded
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# The 4 measurement columns the model uses to predict the species
FEATURES = ['sepal length (cm)', 'sepal width (cm)',
            'petal length (cm)', 'petal width (cm)']


# transformer receives prepare_data's table (150 rows with 'split') as 'df'
# Reads n_neighbors from Mage's pipeline variables, then trains a KNN classifier on the train rows
# Predicts the 30 test rows, prints accuracy, saves the model and returns the predictions
@transformer
def train_knn(df, *args, **kwargs):
    n_neighbors = int(kwargs.get('n_neighbors', 5))

    train_df = df[df['split'] == 'train']
    test_df = df[df['split'] == 'test'].copy()

    model = KNeighborsClassifier(n_neighbors=n_neighbors)
    model.fit(train_df[FEATURES], train_df['target'])

    test_df['predicted_knn'] = model.predict(test_df[FEATURES])
    accuracy = (test_df['predicted_knn'] == test_df['target']).mean()
    print('KNN | n_neighbors =', n_neighbors, '| Test accuracy =', round(accuracy, 3))

    joblib.dump(model, 'iris_knn_model.pkl')
    return test_df


# Runs right after train_knn finishes, using its 30 predicted rows as 'output'
# Recalculates accuracy by comparing 'predicted_knn' with the true 'target'
# If accuracy is below 80%, the block fails and save_results / evaluate_model never run
@test
def test_accuracy(output, *args):
    accuracy = (output['predicted_knn'] == output['target']).mean()
    assert accuracy >= 0.8, 'KNN model accuracy is below 80%'