package persistence;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Date;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Parameter;
import javax.persistence.ParameterMode;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

import org.hibernate.Criteria;
import org.hibernate.FetchMode;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Disjunction;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.sql.JoinType;

import exception.PersistenceException;
import servlet.BaseManager;
import utils.ExceptionUtils;

public class BasePersistence {

	private static EntityManagerFactory entityManagerFactory;
	private EntityManager entityManager;
	private Criteria criteria;
	
	static {
		entityManagerFactory = BaseManager.getEntityManagerFactory();
	}

	public void open() {
		if(this.entityManager == null || !this.entityManager.isOpen())
			this.entityManager = BasePersistence.entityManagerFactory.createEntityManager();
	}

	public void close() {
		if (this.entityManager != null || this.entityManager.isOpen()) {
			this.entityManager.close();
			this.entityManager = null;
		}
	}

	public <T> T create(T entity) {
		try {
			this.entityManager.getTransaction().begin();
			this.entityManager.persist(entity);
			this.entityManager.getTransaction().commit();
		} catch (Exception ex) {
			this.entityManager.getTransaction().rollback();
			throw new PersistenceException(ExceptionUtils.getFullMessage(ex));
		}

		return entity;
	}

	public <T> T update(T entity) {
		try {
			this.entityManager.getTransaction().begin();
			this.entityManager.merge(entity);
			this.entityManager.getTransaction().commit();
		} catch (Exception ex) {
			this.entityManager.getTransaction().rollback();
			throw new PersistenceException(ex.getMessage());
		}

		return entity;
	}

	public void delete(Serializable entity) {
		try {
			this.entityManager.getTransaction().begin();
			this.entityManager.remove(entity);
			this.entityManager.getTransaction().commit();
		} catch (Exception ex) {
			this.entityManager.getTransaction().rollback();
			throw new PersistenceException(ex.getMessage());
		}
	}

	public <T> void deleteById(Class<T> klass, Long id) {
		Serializable entity = (Serializable) findById(klass, id);
		delete(entity);
	}

	public <T> void deleteById(Class<T> klass, Integer id) {
		Serializable entity = (Serializable) findById(klass, id);
		delete(entity);
	}

	public <T> T findById(Class<T> klass, Integer id) {
		return this.entityManager.find(klass, id);
	}

	public <T> T findById(Class<T> klass, Long id) {
		return this.entityManager.find(klass, id);
	}

	public <T> List<T> findAll(Class<T> klass) {
		CriteriaBuilder builder = entityManager.getCriteriaBuilder();
		CriteriaQuery<T> query = builder.createQuery(klass);
		query.from(klass);
		return entityManager.createQuery(query).getResultList();
	}

	public <T> BasePersistence find(Class<T> klass) {
 		Session session = entityManager.unwrap(Session.class);
		criteria = session.createCriteria(klass);
		return this;
	}

	public BasePersistence alias(String propertyName, String alias, JoinType joinType) {
		if (alias != null)
			criteria.createAlias(propertyName, alias, joinType);
		return this;
	}

	public BasePersistence alias(String propertyName, String alias) {
		if (alias != null)
			criteria.createAlias(propertyName, alias, JoinType.LEFT_OUTER_JOIN);
		return this;
	}
	
	public BasePersistence or(Criterion... restrictions) {
		Disjunction disjunction = Restrictions.disjunction();
		for (Criterion r : restrictions) {
			disjunction.add(r);
		}
		criteria.add(disjunction);
		return this;
	}
	
	public BasePersistence eqNotNull(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.eq(propertyName, value));
		return this;
	}
	
	public BasePersistence eq(String propertyName, Object value) {
		criteria.add(Restrictions.eq(propertyName, value));
		return this;
	}
	
	public BasePersistence ne(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.ne(propertyName, value));
		return this;
	}

	public BasePersistence ge(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.ge(propertyName, value));
		return this;
	}

	public BasePersistence le(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.le(propertyName, value));
		return this;
	}

	public BasePersistence and(Criterion lhs, Criterion rhs) {
		if (lhs != null && rhs != null)
			criteria.add(Restrictions.and(lhs, rhs));
		return this;
	}

	public BasePersistence or(Criterion lhs, Criterion rhs) {
		if (lhs != null && rhs != null)
			criteria.add(Restrictions.or(lhs, rhs));
		return this;
	}

	public BasePersistence in(String propertyName, Collection<?> value) {
		if (value != null)
			criteria.add(Restrictions.in(propertyName, value));
		return this;
	}

	public BasePersistence in(String propertyName, Object[] value) {
		if (value != null)
			criteria.add(Restrictions.in(propertyName, value));
		return this;
	}

	public BasePersistence ilike(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.ilike(propertyName, "%" + value + "%"));
		return this;
	}

	public Criterion ilikeC(String propertyName, Object value) {
		if (value != null)
			return Restrictions.ilike(propertyName, "%" + value + "%");
		else
			return null;
	}

	public BasePersistence like(String propertyName, Object value) {
		if (value != null)
			criteria.add(Restrictions.like(propertyName, "%" + value + "%"));
		return this;
	}
	
	public Criterion clike(String propertyName, Object value) {
		if (value != null)
			return Restrictions.like(propertyName, "%" + value + "%");
		else
			return Restrictions.like(propertyName, "%");
	}

	public BasePersistence isEmpty(String propertyName) {
		criteria.add(Restrictions.isEmpty(propertyName));
		return this;
	}

	public BasePersistence isNotEmpty(String propertyName) {
		criteria.add(Restrictions.isNotEmpty(propertyName));
		return this;
	}

	public BasePersistence isNull(String propertyName) {
		criteria.add(Restrictions.isNull(propertyName));
		return this;
	}

	public BasePersistence isNotNull(String propertyName) {
		criteria.add(Restrictions.isNotNull(propertyName));
		return this;
	}

	public BasePersistence between(String propertyName, Object value1, Object value2) {
		if (value1 != null && value2 != null)
			criteria.add(Restrictions.between(propertyName, value1, value2));
		return this;
	}

	public BasePersistence orderByASC(String propertyName) {
		criteria.addOrder(Order.asc(propertyName));
		return this;
	}

	public BasePersistence orderByDESC(String propertyName) {
		criteria.addOrder(Order.desc(propertyName));
		return this;
	}

	public BasePersistence take(Integer quantity) {
		if (quantity != null)
			criteria.setMaxResults(quantity);
		return this;
	}

	public BasePersistence skip(Integer quantity) {
		if (quantity != null)
			criteria.setFirstResult(quantity);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T> T unique() {
		return (T) criteria.uniqueResult();
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> list() {
		return criteria.list();
	}
	
	@SuppressWarnings("unchecked")
	public <T> T firstOrDefault() {
		take(1);
		List<T> results = criteria.list();
		if (results != null && results.size() > 0)
			return results.get(0);
		else
			return null;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> executeQuery(String sqlQuery, String... parameter) {

		Query query = entityManager.createNativeQuery(sqlQuery);

		for (Parameter<?> p : query.getParameters()) {
			query.setParameter(p.getName(), parameter);
		}

		List<T> result = query.getResultList();

		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> executeStoredProcedure(String sqlQuery) {
		open();

		Query query = entityManager.createStoredProcedureQuery(sqlQuery);
		List<T> result = query.getResultList();
		
		close();
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> executeStoredProcedure(String procName, String singleParameterName,
			Class<?> singleParameterClass, Object singleParameterValue) {
		open();

		StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procName);
		query.registerStoredProcedureParameter(singleParameterName, singleParameterClass, ParameterMode.IN);
		query.setParameter(singleParameterName, singleParameterValue);
		List<T> result = query.getResultList();
		
		close();
		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> executeStoredProcedure(String procName, Map<String, Object> parameters) throws Exception {
		open();

		StoredProcedureQuery query = entityManager.createStoredProcedureQuery(procName);

		for (Map.Entry<String, Object> entry : parameters.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			Class<?> valueType = getUnboxedType(value);
			if (valueType == null)
				throw new Exception("Não foi encontrado o tipo (class) para o valor " + value
						+ " para execução da procedure " + procName);

			query.registerStoredProcedureParameter(key, valueType, ParameterMode.IN);
			query.setParameter(key, value);
		}

		List<T> result = query.getResultList();
		
		close();
		return result;

	}

	private Class<?> getUnboxedType(Object boxedObj) {
		Class<?>[] knowTypes = { String.class, BigInteger.class, Integer.class, Double.class, Long.class, Date.class,
				Boolean.class };

		return Arrays.asList(knowTypes).stream().filter(type -> boxedObj.getClass().isAssignableFrom(type)).findFirst()
				.get();
	}

	public BasePersistence fetchMode(String param, FetchMode mode) {
		criteria.setFetchMode(param, mode);
		return this;
	}

	public BasePersistence projection(ProjectionList projectionList) {
		criteria.setProjection(projectionList);
		return this;
	}

	public BasePersistence groupBy(String param) {
		ProjectionList projList = Projections.projectionList();
		projList.add(Projections.groupProperty(param));
		criteria.setProjection(projList);
		return this;
	}

	public Integer count() {
		criteria.setProjection(Projections.rowCount());
		return ((Number) criteria.uniqueResult()).intValue();
	}

	public static void shutdown(){
		if(entityManagerFactory != null){
			entityManagerFactory.close();
			entityManagerFactory = null;
		}
	}

}
