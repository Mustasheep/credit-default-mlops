from setuptools import setup, find_packages

setup(
    name="credit-default-mlops",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "azure-ai-ml>=1.12.0",
        "azure-identity>=1.12.0", 
        "pandas>=1.5.0",
        "numpy>=1.21.0",
        "scikit-learn>=1.2.0",
        "mlflow>=2.0.0",
    ],
    python_requires=">=3.8",
)