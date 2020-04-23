<?php
namespace Digilist\SnakeDumper\Dumper\Sql;

use Digilist\SnakeDumper\Configuration\Table\Filter\DataDependentFilter;
use Digilist\SnakeDumper\Configuration\Table\TableConfiguration;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Driver\Statement;
use Iterator;
use ArrayIterator;

class BatchIterator implements Iterator {

    /**
     * @var int
     */
    protected $size;

    /**
     * @var array
     */
    protected $result;

    /**
     * @var ArrayIterator
     */
    protected $iterator;

    /**
     * @var int
     */
    protected $offset = 0;

    /**
     * @var int
     */
    protected $count = 0;

    /**
     * @var ConnectionHandler
     */
    private $connectionHandler;

    /**
     * @var TableConfiguration
     */
    private $tableConfig;

    /**
     * @var Table
     */
    private $table;

    /**
     * @var array
     */
    private $harvestedValues;

    public function __construct(ConnectionHandler $connectionHandler, TableConfiguration $tableConfig, Table $table, array $harvestedValues, $size = 50) {
        $this->connectionHandler = $connectionHandler;
        $this->tableConfig = $tableConfig;
        $this->table = $table;
        $this->harvestedValues = $harvestedValues;

        $this->size = $size;
    }

    /**
     * @param int $size
     */
    public function setSize($size)
    {
        $this->size = $size;
    }

    /**
     * @return int
     */
    public function getSize()
    {
        return $this->size;
    }

    /**
     * @return array
     */
    protected function loadBatch() {
        list($query, $parameters) = $this->buildSelectQuery($this->tableConfig, $this->table, $this->harvestedValues);

        $result = $this->connectionHandler->getConnection()->prepare($query);
        $result->execute($parameters);

        $results = $result->fetchAll();

        $this->count = count($results);

        $this->offset++;

        return $results;
    }


    /**
     * This method creates the actual select statements and binds the parameters.
     *
     * @param TableConfiguration $tableConfig
     * @param Table              $table
     * @param array              $harvestedValues
     *
     * @return array
     */
    private function buildSelectQuery(TableConfiguration $tableConfig, Table $table, $harvestedValues)
    {
        $qb = $this->createSelectQueryBuilder($tableConfig, $table, $harvestedValues);

        $query = $qb->getSQL();
        $parameters = $qb->getParameters();

        if ($tableConfig->getQuery() != null) {
            $query = $tableConfig->getQuery();

            // Add automatic conditions to the custom query if necessary
            $parameters = [];
            if (strpos($query, '$autoConditions') !== false) {
                $parameters = $qb->getParameters();
                $query = str_replace('$autoConditions', '(' . $qb->getQueryPart('where') . ')', $query);
            }
        }

        return [trim($query), $parameters];
    }

    /**
     * @param TableConfiguration $tableConfig
     * @param Table              $table
     * @param array              $harvestedValues
     *
     * @return QueryBuilder
     */
    private function createSelectQueryBuilder(TableConfiguration $tableConfig, Table $table, $harvestedValues = array())
    {
        $qb = $this->connectionHandler->getConnection()->createQueryBuilder()
            ->select('*')
            ->from($table->getQuotedName($this->connectionHandler->getPlatform()), 't');

        $this->addFiltersToSelectQuery($qb, $tableConfig, $harvestedValues);
        if ($tableConfig->getLimit() != null) {
            $qb->setMaxResults($tableConfig->getLimit());
        }
        if ($tableConfig->getOrderBy() != null) {
            $qb->add('orderBy', $tableConfig->getOrderBy());
        }


        $offset = $this->offset * $this->size;
        $qb->setMaxResults($this->size)
            ->setFirstResult($offset);

        return $qb;
    }

    /**
     * Add the configured filter to the select query.
     *
     * @param QueryBuilder       $qb
     * @param TableConfiguration $tableConfig
     * @param array              $harvestedValues
     */
    private function addFiltersToSelectQuery(QueryBuilder $qb, TableConfiguration $tableConfig, array $harvestedValues)
    {
        $paramIndex = 0;
        foreach ($tableConfig->getFilters() as $filter) {
            if ($filter instanceof DataDependentFilter) {
                $this->handleDataDependentFilter($filter, $tableConfig, $harvestedValues);
            }

            $param = $this->bindParameters($qb, $filter, $paramIndex);
            $expr = call_user_func_array(array($qb->expr(), $filter->getOperator()), array(
                $this->connectionHandler->getPlatform()->quoteIdentifier($filter->getColumnName()),
                $param
            ));

            if ($filter instanceof DataDependentFilter) {
                // also select null values
                $expr = $qb->expr()->orX(
                    $expr,
                    $qb->expr()->isNull(
                        $this->connectionHandler->getPlatform()->quoteIdentifier($filter->getColumnName())
                    )
                );
            }

            $qb->andWhere($expr);

            $paramIndex++;
        }
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Return the current element
     * @link http://php.net/manual/en/iterator.current.php
     * @return mixed Can return any type.
     */
    public function current() {
        return $this->iterator->current();
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Move forward to next element
     * @link http://php.net/manual/en/iterator.next.php
     * @return void Any returned value is ignored.
     */
    public function next() {
        $this->iterator->next();
        if(!$this->iterator->valid()) {

            // If we are in an iteration, but not the first one
            // and the current result is less than the batch size
            // means that there will be no more entities in the database
            // so don't try it.
            // Edge case: overall count is a multiple of batch size
            // Then an additional unnecessary query is emitted
            if($this->offset != 0 && $this->count != $this->size) {
                return;
            }

            $result = $this->loadBatch();
            $this->iterator = new ArrayIterator($result);
        }
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Return the key of the current element
     * @link http://php.net/manual/en/iterator.key.php
     * @return mixed scalar on success, or null on failure.
     */
    public function key() {
        $this->iterator->key();
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Checks if current position is valid
     * @link http://php.net/manual/en/iterator.valid.php
     * @return boolean The return value will be casted to boolean and then evaluated.
     * Returns true on success or false on failure.
     */
    public function valid() {
        $valid = $this->iterator->valid();

        return $valid;
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Rewind the Iterator to the first element
     * @link http://php.net/manual/en/iterator.rewind.php
     * @return void Any returned value is ignored.
     */
    public function rewind() {
        $this->count = 0;
        $this->offset = 0;
        $this->iterator = new ArrayIterator($this->loadBatch());
    }




}
