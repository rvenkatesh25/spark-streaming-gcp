package com.example.prodspark.util

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

object ListUtil {
  /**
    * Implicit class adding the functionality groupedBySize which splits
    * an input list into multiple sub-lists such that size of each
    * sub-list is less than the given size limit
    *
    * Input list needs to be a tuple of form (size, object)
    * Size is explicitly sought instead of accepting a function that can give
    * the size of input object in order to avoid a potentially duplicate
    * serialization of the data.
    *
    * Usage:
    * {{{
    *   val inputList = List((2, "aa"), (3, "bbb"))
    *   val result = inputList.groupedBySize(3)
    * }}}
    * result will be: List(List((2, "aa")), List(3, "bbb"))
    *
    * @param s tuple of (size, object)
    * @tparam T any arbitrary type
    */
  implicit class ListMethods[T : TypeTag](s: List[(Int, T)]) {

    /**
      * @param size size limit of each sub-list
      * @param currentSize size of currentList seen so far in the recursion
      * @param remainingList part of input list yet to be evaluated
      * @param currentList current candidate sub-list
      * @return tuple of (remainingList, currentList) where currentList is a sub
      *         list within the size limits, and remainingList is the rest of
      *         input list yet to be evaluated
      */
    @tailrec
    private def getOneChunk(
      size: Int,
      currentSize: Int,
      remainingList: List[(Int, T)],
      currentList: List[(Int, T)] = List.empty[(Int, T)]
    ): (List[(Int, T)], List[(Int, T)]) = {
      if (remainingList.nonEmpty) {
        // sanity check: a single row shouldn't be bigger than
        // the size limit since it is indivisible
        require(remainingList.head._1 < size,
          "single row bigger than given size limit")

        val newSize = remainingList.head._1 + currentSize
        if (newSize < size) {
          // still within size limit, check more elements in the list
          getOneChunk(size, newSize,  remainingList.tail,  remainingList.head :: currentList)
        } else {
          // found an item that makes the sublist bigger than the limit
          // return the sublist so far, and start over with the rest
          (remainingList, currentList)
        }
      } else {
        // empty input list, return as is
        (remainingList, currentList)
      }
    }

    /**
      * Split input list into sub-lists that are smaller than the size
      * limit in O(n)
      *
      * @param size size limit of each sub-list
      * @param inputList input list of (size, T) tuples
      * @param resultList intermediate list to carry the result during recursion
      * @return list of lists of tuple (size, T) with each
      *         inner list smaller than the size limit
      */
    @tailrec
    final def groupedBySize(
      size: Int,
      inputList: List[(Int, T)] = s,
      resultList: List[List[(Int, T)]] = List.empty[List[(Int, T)]]
    ): List[List[(Int, T)]] = {

      // get the first sub-list within the size limits
      val oneChunk = getOneChunk(size, currentSize = 0, inputList)
      // append the sub-list to the results
      val updatedResultList = oneChunk._2 :: resultList
      // check the remainder of the list
      oneChunk._1.length match {
        // nothing left, return the updated results
        case 0 => updatedResultList
        // add current sublist to results, and recurse over the rest
        case _ =>
          log.warn(s"Payload size greater than $size. " +
            "Splitting into multiple requests")
          groupedBySize(size, oneChunk._1, updatedResultList)
      }
    }

    private val log = LoggerFactory.getLogger(this.getClass)
  }
}
