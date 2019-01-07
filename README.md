# Purpose

Powerful library for handling complex-datatypes in Hive utilizing macros.

#Installation

Clone this repo and build it using gradle, feel free to adjust the hive version to match exactly the one you want it to.
Or download a release from the release version.
 
#Provided Functions

## Reduce Collection

The most powerful abstraction in this library. Compare to 
[Java's Collector](https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collector.html) or 
[PHP's array_reduce()](http://docs.php.net/manual/en/function.array-reduce.php).


### Signature

#### Generics

* `T` Return Type of the macro and the UDF. Will continuously be passed into the macro as parameter 2 or 3, so the macro can use this variable to remember intermediate state.
* `V` the value type of the collection.
* `K` Only present when the collection is a map type. Represents the key of the map.

#### Parameters

* `macroName` needs to be the string name of the macro, It needs to be a string literal and can't be a column references as it is only resolved during compile time.
* `collection` needs to be either a map or an array.
* `initial value`  The initial value contains the type information T and is (for now) required to be non-void. The provided UDF TypeFromString also be used to pass the type information.
* `useInit` An option allows users to pass null to the first invocation, even when a not useful variable had to be passed into initial_value as a type hint.
* `varargs` additional parameters that can be carried through to the macro invocation.

```
T macro constmacroName ([key K], value V, variable T, varargs ....) 

T reduce_collection(String macroName, collection (MAP<K,V> | LIST<V>), initial_value T, boolean useInit, varargs ...)
```

### Examples

Counting the occurrences of a very deeply nested field of type string. The result is a map from string => int containing the number of occurrences.
Execution in Hive will not involve a reduce phase as it would be with `Lateral View explode group by theStringField`.


```sql
CREATE TEMPORARY MACRO updateWordCount(word string, result_map map<string,int>)
trv_udf.update_collection(result_map, word, IF(result_map[word] IS NOT NULL, result_map[word] + 1, 1));

CREATE TEMPORARY MACRO reduce_inner_map(key_integer int, value_strings array<string>, result_map map<string,int>)
IF(key_integer IS NOT NULL, trv_udf.reduce_collection("updateWordCount", value_strings, result_map), result_map);

CREATE TEMPORARY MACRO extract_deeper_field(inputstruct struct<deeper:map<int,array<string>>>, result_map map<string,int>)
trv_udf.reduce_collection("reduce_inner_map", inputstruct.deeper, result_map);

SELECT 
 trv_udf.reduce_collection(
  "extract_deeper_field",
 array(
  named_struct("deeper", map(4, array("a"), 2, array("a","b"))),
  named_struct("deeper", map(24, array("a", "b", "c"), 42, array("a", "b", "c", "d"))),
  named_struct("deeper", map(24, array("a", "b", "c", "d", "e"), 42, array("a", "b", "c", "d", "e", "f")))), map("dummy", 0), false);

=>

{"a":6, "b":5, "c":4, "d":3, "e":2, "f":1}

```

## MAP Collection

Transform elements inside a map or array. If the lambda returns a struct with key and value fields, it will get mapped into a map.

```sql
CREATE TEMPORARY MACRO to_map(value string ) named_struct("key",substr(value,1,1),"value" , substr(value,2,1) * substr(value,2,1))

SELECT map_collection("to_map", array("a2", "b5", "c7", "d9")) 

=> 

{"a":4.0, "b":25.0, "c":49.0, "d":81.0}
```

If the collection is a map, key and value will be passed to the macro.

```
CREATE TEMPORARY MACRO from_map(`key` string, value int ) lpad("", value, `key`)

SELECT map_collection("from_map", map("a", 1, "b", 4, "c", 8))

=>

["a", "bbbb", "cccccccc"]
```

## Filter Collection

Allows removing elements from a collection. The macro return a boolean. True will retain the record in the output, false will remove the record from the output. With maps, no promise is made about the ordering.

### Example

We filter an array so that only the elements are kept that have the square of the number inside the string greater than 80.

```
CREATE TEMPORARY MACRO sqaured_numer_greater_80(value string) substr(value, 2, 1) * substr(value, 2, 1) > 80 

SELECT filter_collection("sqaured_numer_greater_80", array("a2", "b5", "c7", "d9"))

=>

["d9"]
```

## Clear Collection

Removes all the elements from a collection.

```sql
SELECT clear_collection(array(42)) 

=> 

[]
```
