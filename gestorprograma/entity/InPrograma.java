package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
@Table(name = "in_programa")
public class InPrograma implements java.io.Serializable
{

   private static final long serialVersionUID = -8158268424745690343L;
   private String id;
   private String nombre;
   private String descripcion;
   private String idUnidadOrganizacionalPropietaria;
   private String idUsuarioCreacion;
   private String idUsuarioModificacion;
   private Date fecCreacionRegistro;
   private Date fecModificacionRegistro;

   public InPrograma()
   {
   }

   @Id
   @Column(name = "id", unique = true, nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getId()
   {
      return this.id;
   }

   public void setId(String id)
   {
      this.id = id;
   }

   @Column(name = "nombre", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getNombre()
   {
      return this.nombre;
   }

   public void setNombre(String nombre)
   {
      this.nombre = nombre;
   }

   @Column(name = "descripcion", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getDescripcion()
   {
      return this.descripcion;
   }

   public void setDescripcion(String descripcion)
   {
      this.descripcion = descripcion;
   }

   @Column(name = "id_unidad_org_propietaria", nullable = false, length = 50)
   @NotNull
   @Size(max = 50)
   public String getIdUnidadOrganizacionalPropietaria()
   {
      return this.idUnidadOrganizacionalPropietaria;
   }

   public void setIdUnidadOrganizacionalPropietaria(String idUnidadOrganizacionalPropietaria)
   {
      this.idUnidadOrganizacionalPropietaria = idUnidadOrganizacionalPropietaria;
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
