package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity;

import static javax.persistence.GenerationType.IDENTITY;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
@Table(name = "in_programa_regla")
public class InProgramaRegla implements java.io.Serializable
{
   private static final long serialVersionUID = 7663806748596647142L;
   private Long id;
   private String idPrograma;
   private String idRegla;
   private Integer numOrder;
   private Date fecIniVigencia;
   private Date fecFinalVigencia;
   private String idUsuarioCreacion;
   private String idUsuarioModificacion;
   private Date fecCreacionRegistro;
   private Date fecModificacionRegistro;

   public InProgramaRegla()
   {
   }

   @Id
   @GeneratedValue(strategy = IDENTITY)
   @Column(name = "id", unique = true, nullable = false)
   public Long getId()
   {
      return this.id;
   }

   public void setId(Long id)
   {
      this.id = id;
   }

   @Column(name = "id_programa", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getIdPrograma()
   {
      return this.idPrograma;
   }

   public void setIdPrograma(String idPrograma)
   {
      this.idPrograma = idPrograma;
   }

   @Column(name = "id_regla", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getIdRegla()
   {
      return this.idRegla;
   }

   public void setIdRegla(String idRegla)
   {
      this.idRegla = idRegla;
   }

   @Column(name = "num_order")
   public Integer getNumOrder()
   {
      return this.numOrder;
   }

   public void setNumOrder(Integer numOrder)
   {
      this.numOrder = numOrder;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_ini_vigencia", length = 19)
   public Date getFecIniVigencia()
   {
      return this.fecIniVigencia;
   }

   public void setFecIniVigencia(Date fecIniVigencia)
   {
      this.fecIniVigencia = fecIniVigencia;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_fin_vigencia", length = 19)
   public Date getFecFinalVigencia()
   {
      return this.fecFinalVigencia;
   }

   public void setFecFinalVigencia(Date fecFinalVigencia)
   {
      this.fecFinalVigencia = fecFinalVigencia;
   }

   @Column(name = "id_usuario_creacion", length = 50)
   @Size(max = 50)
   public String getIdUsuarioCreacion()
   {
      return this.idUsuarioCreacion;
   }

   public void setIdUsuarioCreacion(String idUsuarioCreacion)
   {
      this.idUsuarioCreacion = idUsuarioCreacion;
   }

   @Column(name = "id_usuario_modificacion", length = 50)
   @Size(max = 50)
   public String getIdUsuarioModificacion()
   {
      return this.idUsuarioModificacion;
   }

   public void setIdUsuarioModificacion(String idUsuarioModificacion)
   {
      this.idUsuarioModificacion = idUsuarioModificacion;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_creacion_registro", length = 19)
   public Date getFecCreacionRegistro()
   {
      return this.fecCreacionRegistro;
   }

   public void setFecCreacionRegistro(Date fecCreacionRegistro)
   {
      this.fecCreacionRegistro = fecCreacionRegistro;
   }

   @Temporal(TemporalType.TIMESTAMP)
   @Column(name = "fec_modificacion_registro", length = 19)
   public Date getFecModificacionRegistro()
   {
      return this.fecModificacionRegistro;
   }

   public void setFecModificacionRegistro(Date fecModificacionRegistro)
   {
      this.fecModificacionRegistro = fecModificacionRegistro;
   }

}
