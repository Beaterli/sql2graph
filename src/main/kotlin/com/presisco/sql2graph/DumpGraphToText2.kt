package com.presisco.sql2graph

import com.presisco.lazyjdbc.client.MapJdbcClient
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.io.File

object DumpGraphToText2 {

    val db = MapJdbcClient(
        HikariDataSource(
            HikariConfig(
                mapOf(
                    "dataSourceClassName" to "com.mysql.cj.jdbc.MysqlDataSource",
                    "dataSource.url" to "jdbc:mysql://localhost:3306/weibo?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8",
                    "dataSource.user" to "root",
                    "dataSource.password" to  "root",
                    "maximumPoolSize" to "5",
                    "connectionTimeout" to "5000"
                ).toProperties()
            )
        )
    )

    val entityFile = "entity2id.txt"

    val relationFile = "relation2id.txt"

    val graphFile = "train2id.txt"

    val trainFile = "train.pairs"

    val testFile = "test.pairs"

    val episodeFile = "episodes.json"

//    val relations = listOf(
//        "keyword", "keyword_inv",
//        "repost", "repost_inv",
//        "comment", "comment_inv",
//        "reference", "reference_inv",
//        "create", "create_inv",
//        "entertainment", "entertainment_inv",
//        "political", "political_inv"
//    )
    val relations = listOf(
            "keyword",
            "repost",
            "comment",
            "reference",
            "create"
    )

    val entertainmentKeywords = setOf(
        "易烊千玺",
        "江一燕",
        "贾玲 情商",
        "雪莉",
        "胡歌 刘涛",
        "少年的你",
        "小丑",
        "#高颜值侧脸照大赛#",
        "双11",
        "天猫双11开幕盛典"
    )

    val politicalKeywords = setOf(
        "10岁女孩被杀",
        "上海 车祸",
        "香港",
        "国庆",
        "阅兵",
        "李心草",
        "智利",
        "朝鲜 火箭炮",
        "未成年人保护法"
    )

    fun getDistinctEntities(table: String, column: String): List<String> {
        val iterator = db.selectIterator("select distinct $column from $table")
        val entities = arrayListOf<String>()
        while (iterator.hasNext()) {
            val entity = iterator.next().getString(column)
            entities.add("${table}_$entity")
        }
        return entities
    }

    fun getEntities(table: String, column: String): List<String> {
        val iterator = db.selectIterator("select $column from $table")
        val entities = arrayListOf<String>()
        while (iterator.hasNext()) {
            val entity = iterator.next()[column].toString()
            entities.add("${table}_$entity")
        }
        return entities
    }

    fun buildEntityIndex(vararg entityGroups: List<String>): Map<String, Int> {
        var index = 0
        val entityToIndex = hashMapOf<String, Int>()

        val writter = File(entityFile).bufferedWriter()

        entityGroups.forEach { group ->
            group.forEach { entity ->
                entityToIndex[entity] = index
                writter.write("$entity\t$index\n")
                index++
            }
        }
        writter.close()
        return entityToIndex
    }

    init {
        val writter = File(relationFile).bufferedWriter()
        relations.forEachIndexed { index, relation -> writter.write("$relation\t$index\n") }
        writter.close()
    }

    fun buildBidirection(from: String, relation: String, to: String) = listOf(
        Triple(from, relation, to)
        //Triple(to, "${relation}_inv", from)
    )

    fun buildKeywordRelation(): List<Triple<String, String, String>> {
        val relationList = arrayListOf<Triple<String, String, String>>()
        db.buildSelect("mid", "keyword")
            .from("root")
            .execute()
            .forEach { row ->
                val from = row.getString("keyword")
                val to = row.getString("mid")
                relationList.addAll(
                    buildBidirection(
                        "root_$from",
                            "keyword",
                        "blog_$to"

                    )
                )
            }
        return relationList
    }

    fun buildRepostRelation(entityToIndex: Map<String, Int>): List<Triple<String, String, String>> {
        val relationList = arrayListOf<Triple<String, String, String>>()
        db.select("SELECT mid, repost_id\n" +
                "FROM `blog`\n" +
                "WHERE repost_id is not null")
            .filter { row ->
                val from = row.getString("repost_id")
                if (!entityToIndex.containsKey("blog_$from")) {
                    println("unknown repost_id: $from")
                    false
                } else {
                    true
                }
            }
            .forEach { row ->
                val from = row.getString("repost_id")
                val to = row.getString("mid")
                relationList.addAll(
                    buildBidirection(
                        "blog_$from",
                            "repost",
                        "blog_$to"
                    )
                )
            }
        return relationList
    }

    fun buildCommentRelation(): List<Triple<String, String, String>> {
        val relationList = arrayListOf<Triple<String, String, String>>()
        db.select("SELECT mid, cid\n" +
                "FROM `comment`\n" +
                "WHERE mid is not null")
            .forEach { row ->
                val from = row.getString("cid")
                val to = row.getString("mid")
                relationList.addAll(
                    buildBidirection(
                        "comment_$from",
                            "comment",
                        "blog_$to"
                    )
                )
            }
        return relationList
    }

    fun buildReferenceRelation(): List<Triple<String, String, String>> {
        val relationList = arrayListOf<Triple<String, String, String>>()
        db.buildSelect("mid", "tid")
            .from("blog_with_tag")
            .execute()
            .forEach { row ->
                val from = row.getString("mid")
                val to = row["tid"].toString()
                relationList.addAll(
                    buildBidirection(
                        "blog_$from",
                            "reference",
                        "tag_$to"
                    )
                )
            }
        return relationList
    }

    fun buildCreateRelation(): List<Triple<String, String, String>> {
        val relationList = arrayListOf<Triple<String, String, String>>()
        db.select("SELECT mid, uid\n" +
                "FROM `blog`\n" +
                "WHERE uid is not null")
            .forEach { row ->
                val from = row.getString("uid")
                val to = row.getString("mid")
                relationList.addAll(
                    buildBidirection(
                        "user_$from",
                            "create",
                        "blog_$to"

                    )
                )
            }
        db.select("SELECT cid, uid\n" +
                "FROM `comment`\n" +
                "WHERE cid is not null")
            .forEach { row ->
                val from = row.getString("uid")
                val to = row.getString("cid")
                relationList.addAll(
                    buildBidirection(
                        "user_$from",
                            "create",
                        "comment_$to"

                    )
                )
            }
        return relationList
    }

    fun dumpRelationAsTrainAndTest(relations: List<Triple<Int, Int, Int>>, testRadio: Float = 0.25f) {
        val shuffled = relations.filter { it.third % 2 == 0 }.shuffled()
        val trainSize = (shuffled.size * (1.0f - testRadio)).toInt()
        val trainSet = shuffled.subList(0, trainSize - 1)
        val testSet = shuffled.subList(trainSize, shuffled.size - 1)
        val trainWritter = File(trainFile).bufferedWriter()
        val testWritter = File(testFile).bufferedWriter()
        trainSet.forEach { trainWritter.write("${it.first}\t${it.second}\t${it.third}\n") }
        testSet.forEach { testWritter.write("${it.first}\t${it.second}\t${it.third}\n") }
        trainWritter.close()
        testWritter.close()
    }

    @JvmStatic
    fun main(vararg args: String) {
        val keywordEntities = getDistinctEntities("root", "keyword")
        val blogEntities = getEntities("blog", "mid")
        val userEntities = getEntities("blog_user", "uid")
        val tagEntities = getEntities("tag", "tid")
        val commentEntities = getEntities("comment", "cid")

        val entityToIndex = buildEntityIndex(keywordEntities, blogEntities, userEntities, commentEntities, tagEntities)

        val relationSet = arrayListOf<Triple<String, String, String>>()
        with(relationSet) {
            addAll(buildKeywordRelation())
            addAll(buildRepostRelation(entityToIndex))
            addAll(buildCommentRelation())
            addAll(buildReferenceRelation())
            addAll(buildCreateRelation())
        }

        val writter = File(graphFile).bufferedWriter()
        relationSet.forEach { triple -> writter.write("${triple.first}\t${triple.second}\t${triple.third}\n") }
        writter.close()
    }


}