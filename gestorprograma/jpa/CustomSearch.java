package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.util.ArrayList;
import java.util.List;

public class CustomSearch<E> implements java.io.Serializable
{
   private static final long serialVersionUID = 702194494098620264L;

   private E entity;
   private String entityString;
   private Boolean orderAsc;
   private String orderBy;
   private Integer maxResult;
   private String dateProperty;
   private List<Long> entityIds;
   private String entityIdsProperty;

   private List<Item> customKeys;

   private List<KeyValue> filterKeys;

   public CustomSearch()
   {

   }

   public void addKey(Item key)
   {
      if (this.customKeys == null)
         this.customKeys = new ArrayList<Item>();

      this.customKeys.add(key);
   }

   public Item findKey(String key)
   {
      if (this.customKeys != null)
      {
         for (Item item : this.customKeys)
         {
            if (key.equals(item.getKey()))
               return item;
         }
      }

      return null;
   }

   public Boolean getOrderAsc()
   {
      return orderAsc;
   }

   public void setOrderAsc(Boolean orderAsc)
   {
      this.orderAsc = orderAsc;
   }

   public String getOrderBy()
   {
      return orderBy;
   }

   public void setOrderBy(String orderBy)
   {
      this.orderBy = orderBy;
   }

   public Integer getMaxResult()
   {
      return maxResult;
   }

   public void setMaxResult(Integer maxResult)
   {
      this.maxResult = maxResult;
   }

   public String getDateProperty()
   {
      return dateProperty;
   }

   public void setDateProperty(String dateProperty)
   {
      this.dateProperty = dateProperty;
   }

   public List<Long> getEntityIds()
   {
      return entityIds;
   }

   public void setEntityIds(List<Long> entityIds)
   {
      this.entityIds = entityIds;
   }

   public E getEntity()
   {
      return entity;
   }

   public void setEntity(E entity)
   {
      this.entity = entity;
   }

   public String getEntityString()
   {
      return entityString;
   }

   public void setEntityString(String entityString)
   {
      this.entityString = entityString;
   }

   public String getEntityIdsProperty()
   {
      return entityIdsProperty;
   }

   public void setEntityIdsProperty(String entityIdsProperty)
   {
      this.entityIdsProperty = entityIdsProperty;
   }

   public List<Item> getCustomKeys()
   {
      return customKeys;
   }

   public void setCustomKeys(List<Item> customKeys)
   {
      this.customKeys = customKeys;
   }

   public List<KeyValue> getFilterKeys()
   {
      return filterKeys;
   }

   public void setFilterKeys(List<KeyValue> filterKeys)
   {
      this.filterKeys = filterKeys;
   }

   public void addFilterKey(KeyValue keyValue)
   {
      if (this.filterKeys == null)
         this.filterKeys = new ArrayList<KeyValue>();

      this.filterKeys.add(keyValue);
   }

}
