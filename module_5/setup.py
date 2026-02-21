from setuptools import setup, find_packages

setup(
    name="gradcafe_app",
    version="1.0.0",
    description="GradCafe analytics Flask application",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "Flask>=3.0,<4",
        "psycopg[binary]>=3.2,<4",
        "beautifulsoup4>=4.12,<5",
    ],
    python_requires=">=3.10",
)