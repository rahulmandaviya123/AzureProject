# Databricks notebook source
class Reusable:

    def drop_columns(self, df, columns):
        df = df.drop(*columns)
        return df

    def dedup(self, df, columns):
        df = df.dropDuplicates(columns)
        return df