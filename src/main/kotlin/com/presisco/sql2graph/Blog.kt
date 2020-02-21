package com.presisco.sql2graph

data class Blog(
    val mid: String,
    val childs: MutableSet<Blog> = hashSetOf(),
    var stage: Int = 0,
    var maxDepth: Int = 0,
    var valid: Boolean = false
) {
    override fun hashCode(): Int {
        return mid.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return when(other){
            is Blog -> this.mid == other.mid
            is String -> this.mid == other
            else -> false
        }
    }

    fun addChild(blog: Blog) = childs.add(blog)

    companion object {

        fun buildWoods(iterator: Iterator<Map<String, *>>): Pair<Map<String, Blog>, Set<Blog>> {
            val blogs = hashMapOf<String, Blog>()
            val roots = hashSetOf<Blog>()
            while (iterator.hasNext()) {
                val row = iterator.next()

                val mid = row.getString("mid")
                if (!blogs.containsKey(mid)) {
                    blogs[mid] = Blog(mid)
                    blogs[mid]!!.valid = true
                }

                if (row["repost_id"] != null) {
                    val repostId = row.getString("repost_id")
                    if (!blogs.containsKey(repostId)) {
                        blogs[repostId] = Blog(repostId)
                        roots.add(blogs[repostId]!!)
                    }
                    roots.remove(blogs[mid]!!)
                    blogs[repostId]!!.addChild(blogs[mid]!!)
                } else {
                    roots.add(blogs[mid]!!)
                }
            }
            return blogs to roots
        }

        fun calcDepths(roots: Set<Blog>): Map<Blog, Int> {
            val depths = hashMapOf<Blog, Int>()
            for (root in roots) {
                depths[root] = maxDepth(root)
            }
            return depths
        }

        fun diffusionWidth(blog: Blog, depth: Int = 0, stages: MutableList<Int> = arrayListOf(0)): List<Int> {
            if (depth == stages.size) {
                stages.add(0)
            }
            stages[depth]++
            blog.childs.forEach { diffusionWidth(it, depth + 1, stages) }
            return stages
        }

        fun maxDepth(blog: Blog, level: Int = 0): Int {
            blog.stage = level
            if (blog.childs.isEmpty()) {
                blog.maxDepth = level
                return level
            }
            blog.childs.forEach { maxDepth(it, level + 1) }
            val deepest = blog.childs.maxBy { it.maxDepth }
            blog.maxDepth = deepest!!.maxDepth
            return blog.maxDepth
        }

        fun longestPath(blog: Blog): MutableList<String> {
            if (blog.childs.isEmpty()) {
                return mutableListOf(blog.mid)
            }

            val deepestChild = blog.childs.maxBy { it.maxDepth }
            val childPath = longestPath(deepestChild!!)
            childPath.add(0, blog.mid)
            return childPath
        }

        fun blogsAtDistance(
            root: Blog,
            distance: Int,
            current: Int = 0,
            blogs: MutableSet<String> = hashSetOf(),
            steps: String = ""
        ): Set<String> {
            val currentSteps = if (steps == "") {
                root.mid
            } else {
                steps + root.mid
            }

            if (distance == current) {
                blogs.add(currentSteps)
                return emptySet()
            }

            if (root.childs.isEmpty())
                return emptySet()

            root.childs.forEach {
                blogsAtDistance(
                    it,
                    distance,
                    current + 1,
                    blogs,
                    "$currentSteps, "
                )
            }

            return if (current == 0)
                blogs
            else
                emptySet()
        }

        fun averageNeighbors(
            root: Blog,
            depth: Int = 0,
            neighbors: MutableSet<Int> = mutableSetOf()
        ): Int {
            if (root.childs.isEmpty()) {
                return 0
            }

            neighbors.add(root.childs.size)
            root.childs.forEach { averageNeighbors(it, depth + 1, neighbors) }

            return if (depth == 0) {
                neighbors.average().toInt()
            } else {
                0
            }
        }
    }
}