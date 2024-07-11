#!/bin/bash

echo "Would you like to run cademycode unsubscriber pipeline? (1/0)"

read run_pipeline

if [ $run_pipeline -eq 1 ]
then
    echo "Running pipeline..."
    python development/pipeline.py
    echo "Pipeline complete."

    # Update Change Log
    echo "Updating Change Log..."
    
    production_changelog_version=$(head -n 1 production/changelog.md)
    development_changelog_version=$(head -n 1 development/changelog.md)

    read -a production_changelog_version_split <<< "$production_changelog_version"
    production_changelog_version=${production_changelog_version_split[1]}

    read -a development_changelog_version_split <<< "$development_changelog_version"
    development_changelog_version=${development_changelog_version_split[1]}

    if [ $development_changelog_version != $production_changelog_version ]
    then
        echo "Development version is not equal to production version. Continue running? [1/0]"
        read continue
    else
        continue=0
    fi

else
    echo "Pipeline cancelled."
fi

if [ $continue -eq 1 ]
then
    for filename in data/*
    do
        if [ $filename == "data/cademycode_cleansed.db" ] || [ $filename == "data/cademycode_cleansed.csv" ]
        then
            cp $filename prod
            echo "Copying " $filename
        else
            echo "Not Copying " $filename
        fi
    done

    for filename in development/*
    do
        if [ $filename == "development/changelog.md" ]
        then
            cp $filename prod
            echo "Copying " $filename
        else
            echo "Not Copying " $filename
        fi
    done
else
    echo "Please come back when you are ready"
fi

