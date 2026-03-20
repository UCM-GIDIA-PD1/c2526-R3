from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import fbeta_score
import wandb
import yaml


def decision_tree(X_train, X_val, X_test, y_train, y_val, y_test, config):
    tree = DecisionTreeClassifier(
        max_depth=config.max_depth,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        random_state=42
    )
    tree.fit(X_train, y_train)