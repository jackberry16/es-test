package com.chengh.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class User_Query {
    public static void main(String[] args) throws Exception {
        //创建ES客户端
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        //1.全量查询matchAllQuery
//        queryAll(restHighLevelClient);

        //2.条件查询termQuery
//        queryByField(restHighLevelClient);

        //3.分页查询
//        queryByPage(restHighLevelClient);

        //4.过滤查询
//        queryByFilter(restHighLevelClient);

        //5.组合查询
//        queryBool(restHighLevelClient);

        //6.模糊查询
        queryFuzzy(restHighLevelClient);

        //关闭ES客户端
        restHighLevelClient.close();
    }

    private static void queryAll(RestHighLevelClient restHighLevelClient) throws Exception {
        //查询
        SearchRequest request = new SearchRequest();
        request.indices("user");

        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }

    private static void queryByField(RestHighLevelClient restHighLevelClient) throws Exception {
        //查询
        SearchRequest request = new SearchRequest();
        request.indices("user");

        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age", 19)));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }

    private static void queryByPage(RestHighLevelClient restHighLevelClient) throws Exception {
        //查询
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //分页
        builder.from(0);
        builder.size(5);
        //排序
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }

    private static void queryByFilter(RestHighLevelClient restHighLevelClient) throws Exception {
        //查询
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());

        String[] excludes = {"age"};
        String[] includes = {};
        builder.fetchSource(includes, excludes);

        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }

    private static void queryBool(RestHighLevelClient restHighLevelClient) throws Exception {

        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();

        //组合查询
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.matchQuery("age", 21));
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex", "女"));
//        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 18));

        //范围查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gte(20);
        rangeQueryBuilder.lte(30);

        builder.query(rangeQueryBuilder);

        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }

    private static void queryFuzzy(RestHighLevelClient restHighLevelClient) throws Exception {

        SearchRequest request = new SearchRequest();
        request.indices("user");

        SearchSourceBuilder builder = new SearchSourceBuilder();

        builder.query(QueryBuilders.fuzzyQuery("name", "张").fuzziness(Fuzziness.ZERO));

        request.source(builder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());

        hits.forEach(a -> System.out.println(a.getSourceAsString()));
    }


}
