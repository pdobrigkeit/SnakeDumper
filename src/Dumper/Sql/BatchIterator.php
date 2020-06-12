<?php
namespace Digilist\SnakeDumper\Dumper\Sql;

use Digilist\SnakeDumper\Configuration\Table\Filter\DataDependentFilter;
use Digilist\SnakeDumper\Configuration\Table\Filter\DefaultFilter;
use Digilist\SnakeDumper\Configuration\Table\Filter\FilterInterface;
use Digilist\SnakeDumper\Configuration\Table\TableConfiguration;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Driver\Statement;
use Iterator;
use ArrayIterator;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

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
     * Validates and modifies the data dependent filter to act like an IN-filter.
     *
     * @param DataDependentFilter $filter
     * @param TableConfiguration               $tableConfig
     * @param array                            $harvestedValues
     */
    private function handleDataDependentFilter(
        DataDependentFilter $filter,
        TableConfiguration $tableConfig,
        array $harvestedValues
    ) {
        $tableName = $tableConfig->getName();
        $referencedTable = $filter->getReferencedTable();
        $referencedColumn = $filter->getReferencedColumn();

        // Ensure the dependent table has been dumped before the current table
        if (!isset($harvestedValues[$referencedTable])) {
            throw new InvalidArgumentException(
                sprintf(
                    'The table %s has not been dumped before %s',
                    $referencedTable,
                    $tableName
                )
            );
        }

        // Ensure the necessary column was included in the dump
        if (!isset($harvestedValues[$referencedTable][$referencedColumn])) {
            throw new InvalidArgumentException(
                sprintf(
                    'The column %s of table %s has not been dumped.',
                    $referencedTable,
                    $tableName
                )
            );
        }

        $filter->setValue($harvestedValues[$referencedTable][$referencedColumn]);
    }

    /**
     * Binds the parameters of the filter into the query builder.
     *
     * This function returns false, if the condition is not fulfill-able and no row can be selected at all.
     *
     * @param QueryBuilder    $qb
     * @param FilterInterface $filter
     * @param int             $paramIndex
     *
     * @return array|string|bool
     */
    private function bindParameters(QueryBuilder $qb, FilterInterface $filter, $paramIndex)
    {
        $inOperator = in_array($filter->getOperator(), [
            DefaultFilter::OPERATOR_IN,
            DefaultFilter::OPERATOR_NOT_IN,
        ]);

        if ($inOperator) {
            // the IN and NOT IN operator expects an array which needs a different handling
            // -> each value in the array must be mapped to a single param

            $values = (array) $filter->getValue();
            if (empty($values)) {
                $values = array('_________UNDEFINED__________');
            }

            $param = array();
            foreach ($values as $valueIndex => $value) {
                $tmpParam = 'param_' . $paramIndex . '_' . $valueIndex;
                $param[] = ':' . $tmpParam;

                $qb->setParameter($tmpParam, $value);
            }
        } else {
            $param = ':param_' . $paramIndex;

            $qb->setParameter('param_' . $paramIndex, $filter->getValue());
        }

        return $param;
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
        return $this->iterator->key();
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
