This project captures a full data analytics cycle, touching on major aspects such as data ingestion, transformation, analytics, loading, to cloud storage and finally visualization.

Project title: Customer Transaction Analytics Sector: Banking and Finance Project duration: 1 week Data source: kaggle Bank customer transactiona dataset by

Project Phases: The project will move through teh following stages

Stage 1: Requirement analysis

  Here we shall touch on the following tasks:
  
  Problem statement:
  
  Objectives

  KPIs
  
  Identify data source
Stage 2: Data ingestion into postgresdb with python Stage 3: Data cleaning and transformation with sql Stage 4: Data Analytics and loading Stage 5: Data visualization and presentation

Tools used: Python SQL Power BI Azure blob

Env setup: project directory and dependencies where handled using uv package manager.

        After cloning the repository, 
        
        on your terminal enter: uv .venv to create virtual env
        
        on your terminal enter 'uv sync' this will automatically install the dependencies
        
        if you do not have uv installed, 
        on your terminal enter: curl -LsSf https://astral.sh/uv/install.sh | sh (for mac PC)
        for global installation on your mac you can use: brew install uv
        and 
        powershell -c "irm https://astral.sh/uv/install.ps1 | iex" (for windows PC)

        initialize uv with uv init

        install dependencies using uv sync


        After this, you should be all setup