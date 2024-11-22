from setuptools import setup, find_packages

# Lê o arquivo requirements.txt
with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

setup(
    name="case-generate-vendas",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=install_requires, 
    description="Pipeline para gerar vendas usando PySpark",
    author="Matheus Brito",
    author_email="matheusnunesdebrito@gmail.com",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
