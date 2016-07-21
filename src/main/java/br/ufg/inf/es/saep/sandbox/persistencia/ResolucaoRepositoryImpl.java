package br.ufg.inf.es.saep.sandbox.persistencia;

import br.ufg.inf.es.saep.sandbox.dominio.CampoExigidoNaoFornecido;
import br.ufg.inf.es.saep.sandbox.dominio.Resolucao;
import br.ufg.inf.es.saep.sandbox.dominio.ResolucaoRepository;
import br.ufg.inf.es.saep.sandbox.dominio.Tipo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 * Classe que implementa a persistência segundo a interface
 * {@code ResolucaoRepository}.
 * 
 * @see ResolucaoRepository
 */
public class ResolucaoRepositoryImpl implements ResolucaoRepository {

	private final MongoDatabase db;
	static Gson gson = new GsonBuilder().serializeNulls().create();

	/**
	 * Construtor da Implementação da interface {@link ResolucaoRepository}.
	 * 
	 * @param db
	 *            Uma instancia do banco MongoDB onde será feira a persistência.
	 */
	public ResolucaoRepositoryImpl(MongoDatabase db) {
		if (db == null) {
			throw new CampoExigidoNaoFornecido("db");
		}
		this.db = db;
	}

	@Override
	public Resolucao byId(String id) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.RESOLUCAO_COLLECTION).find(
				new Document("_id", id));

		if (it.iterator().hasNext()) {
			String resolucaoJson = it.iterator().next().toJson()
					.replaceFirst("_id", "id");
			return gson.fromJson(resolucaoJson, Resolucao.class);
		}
		return null; // não há Resolucão com esse id
	}

	@Override
	public String persiste(Resolucao resolucao) {

		if (resolucao == null) {
			throw new CampoExigidoNaoFornecido("resolucao");
		}

		Iterable<Document> it = db.getCollection(
				MongoConstantes.RESOLUCAO_COLLECTION).find(
				new Document("_id", resolucao.getId()));

		if (it.iterator().hasNext()) {
			return null;
		}

		String resolucaoJson = gson.toJson(resolucao);
		resolucaoJson = resolucaoJson.replaceFirst("id", "_id");

		Document resolucaoDocument = Document.parse(resolucaoJson);

		db.getCollection(MongoConstantes.RESOLUCAO_COLLECTION).insertOne(
				resolucaoDocument);

		return resolucao.getId();
	}

	@Override
	public boolean remove(String identificador) {

		DeleteResult res = db.getCollection(
				MongoConstantes.RESOLUCAO_COLLECTION).deleteOne(
				new Document("_id", identificador));

		if (res.getDeletedCount() == 0) { // nenhuma Resolução com esse id
			return false;
		} else {
			return true;
		}
	}

	@Override
	public List<String> resolucoes() {
		List<String> ids = new ArrayList<>();

		for (Document resolucao : db.getCollection(
				MongoConstantes.RESOLUCAO_COLLECTION).find()) {
			ids.add(resolucao.getString("_id"));
		}
		return ids;
	}

	@Override
	public void persisteTipo(Tipo tipo) {
		String tipoJson = gson.toJson(tipo).replaceFirst("id", "_id");

		db.getCollection(MongoConstantes.TIPO_COLLECTION).insertOne(
				Document.parse(tipoJson));
	}

	@Override
	public void removeTipo(String codigo) {
		db.getCollection(MongoConstantes.TIPO_COLLECTION).deleteOne(
				new Document("_id", codigo));
	}

	@Override
	public Tipo tipoPeloCodigo(String codigo) {
		Iterable<Document> it = db.getCollection(
				MongoConstantes.TIPO_COLLECTION).find(
				new Document("_id", codigo));

		if (it.iterator().hasNext()) {
			String tipoJson = it.iterator().next().toJson()
					.replaceFirst("_id", "id");
			return gson.fromJson(tipoJson, Tipo.class);
		}
		return null; // não há Tipo com esse id
	}

	@Override
	public List<Tipo> tiposPeloNome(String nome) {
		List<Tipo> list = new ArrayList<>();
		Document regex = new Document();
		regex.append("$regex", nome); // Usa uma feature do proprio MongoDB pra
										// buscar o nome
		// Ele procura o nome como uma sub-string do campo
		regex.append("$options", "i"); // 'i' é para a busca ser
										// case-insensitive

		Document findQuery = new Document();
		findQuery.append("nome", regex);

		for (Document doc : db.getCollection(MongoConstantes.TIPO_COLLECTION)
				.find(findQuery)) {

			String tipoJson = doc.toJson().replaceFirst("_id", "id");
			Tipo t = gson.fromJson(tipoJson, Tipo.class);
			list.add(t);
		}
		return list;
	}
}
