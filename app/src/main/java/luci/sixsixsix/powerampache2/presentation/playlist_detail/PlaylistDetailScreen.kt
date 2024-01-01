package luci.sixsixsix.powerampache2.presentation.playlist_detail

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.Divider
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.hilt.navigation.compose.hiltViewModel
import com.google.accompanist.swiperefresh.SwipeRefresh
import com.google.accompanist.swiperefresh.rememberSwipeRefreshState
import com.ramcosta.composedestinations.annotation.Destination
import com.ramcosta.composedestinations.navigation.DestinationsNavigator
import luci.sixsixsix.powerampache2.domain.models.Playlist
import luci.sixsixsix.powerampache2.presentation.album_detail.AlbumDetailEvent
import luci.sixsixsix.powerampache2.presentation.destinations.AlbumDetailScreenDestination
import luci.sixsixsix.powerampache2.presentation.destinations.ArtistDetailScreenDestination
import luci.sixsixsix.powerampache2.presentation.songs.components.SongInfoThirdRow

import luci.sixsixsix.powerampache2.presentation.songs.components.SongItem
import luci.sixsixsix.powerampache2.presentation.songs.components.SongItemEvent

@Composable
@Destination
fun PlaylistDetailScreen(
    navigator: DestinationsNavigator,
    playlist: Playlist,
    viewModel: PlaylistDetailViewModel = hiltViewModel(),
    modifier: Modifier = Modifier
) {
    val swipeRefreshState = rememberSwipeRefreshState(isRefreshing = viewModel.state.isRefreshing)
    val state = viewModel.state

    Column(
        modifier = Modifier.fillMaxSize()
    ) {
        SwipeRefresh(
            state = swipeRefreshState,
            onRefresh = { viewModel.onEvent(PlaylistDetailEvent.Refresh) }
        ) {
            LazyColumn(modifier = Modifier.fillMaxSize()) {
                items(state.songs.size) { i ->
                    val song = state.songs[i]
                    SongItem(
                        song = song,
                        songItemEventListener = { event ->
                            when(event) {
                                SongItemEvent.PLAY_NEXT -> {} // viewModel.onEvent(AlbumDetailEvent.OnAddSongToQueueNext(song))
                                SongItemEvent.SHARE_SONG -> {} // viewModel.onEvent(AlbumDetailEvent.OnShareSong(song))
                                SongItemEvent.DOWNLOAD_SONG -> {} // viewModel.onEvent(AlbumDetailEvent.OnDownloadSong(song))
                                SongItemEvent.GO_TO_ALBUM -> {} //  navigator.navigate(AlbumDetailScreenDestination(albumId = song.album.id))
                                SongItemEvent.GO_TO_ARTIST -> navigator.navigate(
                                    ArtistDetailScreenDestination(artistId = song.artist.id, artist = null)
                                )
                                SongItemEvent.ADD_SONG_TO_QUEUE -> {} // viewModel.onEvent(AlbumDetailEvent.OnAddSongToQueue(song))
                                SongItemEvent.ADD_SONG_TO_PLAYLIST -> {}
                            }
                        },
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable {
                                viewModel.onEvent(PlaylistDetailEvent.OnSongSelected(song))
                                //mainViewModel.state = mainViewModel.state.copy(currentSong = song)
                            }
                            .padding(16.dp),
                        songInfoThirdRow = SongInfoThirdRow.Time
                    )

                    if (i < state.songs.size - 1) {
                        // if not last item add a divider
                        Divider(modifier = Modifier.padding(horizontal = 16.dp))
                    }
                }
            }
        }
    }
}
