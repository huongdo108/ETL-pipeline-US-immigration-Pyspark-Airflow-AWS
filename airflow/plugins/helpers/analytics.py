class ImmigrationAnalytics:
    @classmethod
    def query_top_origin_countries(self):
        return """SELECT i94cit, COUNT(i94cit)
        FROM production.fact_immigration_events
        GROUP BY i94cit
        ;
        """

    @classmethod
    def query_top_destination_cities(self):
        return """SELECT i94addr,city,COUNT(city)
        FROM production.fact_immigration_events a
        INNER JOIN production.dim_cities_demographics_summary b
        ON a.i94addr = b.state_code
        GROUP BY i94addr,city
        ;
        """

    @classmethod
    def query_reason_visit_by_country(self):
        return """SELECT i94cit, visatype, COUNT(visatype)
        FROM production.fact_immigration_events
        GROUP BY i94cit, visatype
        ;
        """
